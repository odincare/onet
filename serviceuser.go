package onet

import (
	"fmt"
	"github.com/odincare/odicom"
	"github.com/odincare/odicom/dicomio"
	"github.com/odincare/odicom/dicomtag"
	"net"
	"sync"

	"github.com/sirupsen/logrus"
)

type serviceUserStatus int

const (
	serviceUserInitial = iota
	serviceUserAssociationActive
	serviceUserClosed
)

// ServiceUser 封装实现DICOM协议的SCU
//
// ServiceUser 类是线程兼容的。
// 也就是说，你不能同时从两个 goroutine 调用 C* 方法——比如 CStore 和 CFind 请求。
// 在发出 CFind 之前，您必须等待 CStore 完成。
type ServiceUser struct {
	label    string // 打印用
	upcallCh chan upcallEvent

	mutex *sync.Mutex
	cond  *sync.Cond // status改变是广播
	disp  *serviceDispatcher

	// 剩下的由mutex守护
	status serviceUserStatus
	// 只在三次握手完成后设置
	cm *contextManager
}

// ServiceUserParams 定义了ServiceUser的参数.
type ServiceUserParams struct {
	// Application-entity title of the peer. If empty, set to "unknown-called-ae"
	CalledAETitle string
	// Application-entity title of the client. If empty, set to
	// "unknown-calling-ae"
	CallingAETitle string

	// List of SOPUIDs wanted by the client. The value is typically one of
	// the constants listed in sopclass package.
	SOPClasses []string

	// List of Transfer syntaxes supported by the user.  If you know the
	// transer syntax of the file you are going to copy, set that here.
	// Otherwise, you'll need to re-encode the data w/ the given transfer
	// syntax yourself.
	//
	// TODO(saito) Support reencoding internally on C_STORE, etc. The DICOM
	// spec is particularly moronic here, since we could just have specified
	// the transfer syntax per data sent.
	TransferSyntaxes []string
}

// CFindResult 是通过CFind流式传输的对象
type CFindResult struct {
	// 具体一个错误或者一个Element
	Err      error
	Elements []*dicom.Element
}

// validateServiceUserParams 验证SCU的params是否ok
func validateServiceUserParams(params *ServiceUserParams) error {
	if params.CalledAETitle == "" {
		params.CalledAETitle = "UNKNOWN-CALLED-AE"
	}

	if params.CallingAETitle == "" {
		params.CallingAETitle = "UNKNOWN-CALLING-AE"
	}

	if len(params.SOPClasses) == 0 {
		return fmt.Errorf("Empty ServiceUserParams.SOPClasses")
	}

	if len(params.TransferSyntaxes) == 0 {
		params.TransferSyntaxes = dicomio.StandardTransferSyntaxes
	} else {
		for i, uid := range params.TransferSyntaxes {
			canonicalUID, err := dicomio.CanonicalTransferSyntaxUID(uid)
			if err != nil {
				return err
			}

			params.TransferSyntaxes[i] = canonicalUID
		}
	}

	return nil
}

// NewServiceUser 创建一个新的ServiceUser. 在调用其他方法前，必须先调用Connect() 或 SetConn()，如C-store
func NewServiceUser(params ServiceUserParams) (*ServiceUser, error) {
	if err := validateServiceUserParams(&params); err != nil {
		return nil, err
	}

	mutex := &sync.Mutex{}

	label := newUID("user")

	su := &ServiceUser{
		label:    label,
		upcallCh: make(chan upcallEvent, 128),
		disp:     newServiceDispatcher(label),
		mutex:    mutex,
		cond:     sync.NewCond(mutex),
		status:   serviceUserInitial,
	}

	go runStateMachineForServiceUser(params, su.upcallCh, su.disp.downcallCh, label)

	go func() {
		for event := range su.upcallCh {
			if event.eventType == upcallEventHandshakeCompleted {
				su.mutex.Lock()
				dicomio.DoAssert(su.cm == nil)
				su.status = serviceUserAssociationActive
				su.cond.Broadcast()
				su.cm = event.cm
				dicomio.DoAssert(su.cm != nil)
				su.mutex.Unlock()
				continue
			}

			dicomio.DoAssert(event.eventType == upcallEventData)

			su.disp.handleEvent(event)
		}

		logrus.Info("dicom_server.serviceUser: dispatcher 完成")
		su.disp.close()
		su.mutex.Lock()
		su.cond.Broadcast()
		su.status = serviceUserClosed
		su.mutex.Unlock()
	}()

	return su, nil
}

func (su *ServiceUser) waitUntilReady() error {
	su.mutex.Lock()
	defer su.mutex.Unlock()
	for su.status <= serviceUserInitial {
		su.cond.Wait()
	}

	if su.status != serviceUserAssociationActive {
		// 将会在等待response时返回一个错误
		logrus.Error("dicom_server.serviceUser: 连接失败")
		return fmt.Errorf("dicom_server.serviceUser: 连接失败")
	}

	return nil
}

// Connect 由传来的 serverAddr("host:port")连接server
// Connect或SetConn必须在CStore调用前调用，etc
func (su *ServiceUser) Connect(serverAddr string) {
	if su.status != serviceUserInitial {
		logrus.Panic(fmt.Errorf("dicom_server.serviceUser: 连接调用了错误的状态: %v", su.status))
	}

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		logrus.Errorf("dicom_server.serviceUser: Connect(%s): %v", serverAddr, err)
		su.disp.downcallCh <- stateEvent{event: evt17, pdu: nil, err: err}
	} else {
		su.disp.downcallCh <- stateEvent{event: evt02, pdu: nil, err: nil, conn: conn}
	}
}

// CStore 发起一个C-Store请求来传递’ds'到remove peer
// 它会拦截其他请求直到操作完成
// 注意： 必须在调用前调用Connect()或SetConn（）
func (su *ServiceUser) CStore(ds *dicom.DataSet) error {
	err := su.waitUntilReady()
	if err != nil {
		return err
	}

	dicomio.DoAssert(su.cm != nil)

	var sopClassUID string

	if sopClassUIDElement, err := ds.FindElementByTag(dicomtag.MediaStorageSOPClassUID); err != nil {
		return err
	} else if sopClassUID, err = sopClassUIDElement.GetString(); err != nil {
		return err
	}

	context, err := su.cm.lookupByAbstractSyntaxUID(sopClassUID)
	if err != nil {
		return err
	}

	cs, err := su.disp.newCommand(su.cm, context)
	if err != nil {
		logrus.Errorf("dicom_server.serviceUser: C-STORE: sop class %v 没有在context中找到,  %v", sopClassUID, err)
		return err
	}

	defer su.disp.deleteCommand(cs)

	return runCStoreOnAssociation(cs.upcallCh, su.disp.downcallCh, su.cm, cs.messageID, ds)
}

// Release 关闭链接，它必须只调用一次
// 在Release()之后，就不再能作为ServiceUser使用
func (su *ServiceUser) Release() {
	su.disp.downcallCh <- stateEvent{event: evt11}
	su.mutex.Lock()
	defer su.mutex.Unlock()
	su.status = serviceUserClosed
	su.cond.Broadcast()
	su.disp.close()
}
