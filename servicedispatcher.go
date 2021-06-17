package onet

import (
	"fmt"
	"github.com/odincare/odicom/dicomio"
	"github.com/odincare/onet/dimse"
	"sync"

	"github.com/sirupsen/logrus"
)

// serviceDispatcher multiplexes statemachine upcall events to DIMSE commands
type serviceDispatcher struct {
	// 打印用
	label string
	// 用来发送PDUs 到 statemachine
	downcallCh chan stateEvent

	mutex sync.Mutex

	// 可用的&运行的DIMSE命令集，key是message IDs
	// 由mutex守护
	activeCommands map[dimse.MessageID]*serviceCommandState

	// dimse请求到达时的callback, key是DIMSE的 commandField。
	// callback一般由calling中的findOrCreateCommand创建
	// 由mutex守护
	callbacks map[int]serviceCallback

	// newCommand()使用最后一个messageId，用来避免创建重复的MessageId
	lastMessageID dimse.MessageID
}

type serviceCallback func(msg dimse.Message, data []byte, cs *serviceCommandState)

// Per-DIMSE-command state.
// 每个DIMSE命令的状态？
type serviceCommandState struct {
	disp      *serviceDispatcher  // Parent.
	messageID dimse.MessageID     // Command's MessageID.
	context   contextManagerEntry // Transfersyntax/sopclass for this command.
	cm        *contextManager     // For looking up context -> transfersyntax/sopclass mappings

	// upcallCh streams command+data for this messageID.
	upcallCh chan upcallEvent
}

type stateEventDebugInfo struct {
	state stateType // the state the system was in when timer was created.
}

// 发送一个command+data组合给远程peer， data可能为nil
func (cs *serviceCommandState) sendMessage(cmd dimse.Message, data []byte) {
	if s := cmd.GetStatus(); s != nil && s.Status != dimse.StatusSuccess && s.Status != dimse.StatusPending {
		logrus.Warnf("dicom_service.serviceDispatcher(%s): sending DIMSE error: %v %v", cs.disp.label, cmd, cs.disp)
	} else {
		logrus.Infof("dicom.serviceDispatcher(%s): Sending DIMSE message: %v %v", cs.disp.label, cmd, cs.disp)
	}

	payload := &stateEventDIMSEPayload{
		abstractSyntaxName: cs.context.abstractSyntaxUID,
		command:            cmd,
		data:               data,
	}

	cs.disp.downcallCh <- stateEvent{
		event:        evt09,
		pdu:          nil,
		conn:         nil,
		dimsePayload: payload,
	}
}

func (disp *serviceDispatcher) findOrCreateCommand(
	msgID dimse.MessageID,
	cm *contextManager,
	context contextManagerEntry) (*serviceCommandState, bool) {
	disp.mutex.Lock()
	defer disp.mutex.Unlock()
	if cs, ok := disp.activeCommands[msgID]; ok {
		return cs, true
	}
	cs := &serviceCommandState{
		disp:      disp,
		messageID: msgID,
		cm:        cm,
		context:   context,
		upcallCh:  make(chan upcallEvent, 128),
	}
	disp.activeCommands[msgID] = cs
	logrus.Infof("dicom.serviceDispatcher(%s): Start command %+v", disp.label, cs)
	return cs, false
}

// newCommand 创建一个新的serviceCommandState 带着一个没有使用过的message ID.
// 如果分配MessageID失败则返回一个错误
func (disp *serviceDispatcher) newCommand(
	cm *contextManager, context contextManagerEntry) (*serviceCommandState, error) {
	disp.mutex.Lock()
	defer disp.mutex.Unlock()

	for msgID := disp.lastMessageID + 1; msgID != disp.lastMessageID; msgID++ {
		if _, ok := disp.activeCommands[msgID]; ok {
			continue
		}

		cs := &serviceCommandState{
			disp:      disp,
			messageID: msgID,
			cm:        cm,
			context:   context,
			upcallCh:  make(chan upcallEvent, 128),
		}
		disp.activeCommands[msgID] = cs
		disp.lastMessageID = msgID
		logrus.Infof("dicom_server.serviceDispatcher: 开启新的command： %+v", cs)
		return cs, nil
	}
	return nil, fmt.Errorf("分配message ID 失败 (已经有太多存在的了?)")
}

func (disp *serviceDispatcher) deleteCommand(cs *serviceCommandState) {
	disp.mutex.Lock()
	logrus.Infof("dicom_server.serviceDispatcher(%s): Finish provider command %v", disp.label, cs.messageID)
	if _, ok := disp.activeCommands[cs.messageID]; !ok {
		logrus.Panic(fmt.Sprintf("cs %+v", cs))
	}
	delete(disp.activeCommands, cs.messageID)
	disp.mutex.Unlock()
}

func (disp *serviceDispatcher) registerCallback(commandField int, cb serviceCallback) {
	disp.mutex.Lock()
	disp.callbacks[commandField] = cb
	disp.mutex.Unlock()
}

func (disp *serviceDispatcher) unregisterCallback(commandField int) {
	disp.mutex.Lock()
	delete(disp.callbacks, commandField)
	disp.mutex.Unlock()
}

// 处理请求
func (disp *serviceDispatcher) handleEvent(event upcallEvent) {
	if event.eventType == upcallEventHandshakeCompleted {
		return
	}

	dicomio.DoAssert(event.eventType == upcallEventData)
	dicomio.DoAssert(event.command != nil)
	context, err := event.cm.lookupByContextID(event.contextID)
	if err != nil {
		logrus.Warnf("dicom_server.serviceDispatcher(%s): Invalid context ID %d: %v", disp.label, event.contextID, err)
		disp.downcallCh <- stateEvent{event: evt19, pdu: nil, err: err}
		return
	}

	messageID := event.command.GetMessageID()
	dc, found := disp.findOrCreateCommand(messageID, event.cm, context)
	if found {
		logrus.Infof("dicom_service.serviceDispatcher(%s): Forwarding command to existing command: %+v %+v", disp.label, event.command, dc)
		dc.upcallCh <- event
		logrus.Infof("dicom_service.serviceDispatcher(%s): Done forwarding command to existing command: %+v %+v", disp.label, event.command, dc)
		return
	}

	disp.mutex.Lock()
	cb := disp.callbacks[event.command.CommandField()]
	disp.mutex.Unlock()

	go func() {
		cb(event.command, event.data, dc)
		disp.deleteCommand(dc)
	}()

}

// 一定要一次性关闭dispatcher
func (disp *serviceDispatcher) close() {
	disp.mutex.Lock()
	for _, cs := range disp.activeCommands {
		close(cs.upcallCh)
	}
	disp.mutex.Unlock()
	// TODO prevent new command from launching.
}

func newServiceDispatcher(label string) *serviceDispatcher {
	return &serviceDispatcher{
		label:          label,
		downcallCh:     make(chan stateEvent, 128),
		activeCommands: make(map[dimse.MessageID]*serviceCommandState),
		callbacks:      make(map[int]serviceCallback),
		lastMessageID:  123,
	}
}
