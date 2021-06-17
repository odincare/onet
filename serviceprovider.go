package onet

import (
	"crypto/tls"
	"fmt"
	"github.com/odincare/odicom"
	"github.com/odincare/odicom/dicomio"
	"github.com/odincare/onet/dimse"
	"github.com/odincare/onet/sopclass"
	"net"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

var idSeq int32 = 32 // for generating unique ID

type ConnectionState struct {
	// TLS connection state. It is nonempty only when the connection is set up
	// over TLS.
	TLS tls.ConnectionState
}

// ServiceProviderParams defines parameters for ServiceProvider.
type ServiceProviderParams struct {
	// The application-entity title of the server. Must be nonempty
	AETitle string

	// Names of remote AEs and their host:ports. Used only by C-MOVE. This
	// map should be nonempty iff the server supports CMove.
	RemoteAEs map[string]string

	// C_ECHO 请求调用. If nil, a C-ECHO call will produce an error response.
	//
	// 支持一个默认的c-echo？
	CEcho CEchoCallback

	// C-FIND 请求调用
	// 如果 CFindCallback返回nil， c-find会返回一个error response
	// If CFindCallback=nil, a C-FIND call will produce an error response.
	CFind CFindCallback

	// C_MOVE 请求调用.
	CMove CMoveCallback

	// C-GET请求调用 c-move和c-get只有一个区别：
	// c-get使用相同的连接来发送图片
	// c-move会创建一条新的连接来（一起？）发送
	// 通常来说你应该对c-move和c-get使用同样的函数
	CGet CMoveCallback

	// CStore 请求调用
	// 如果 CStoreCallback=nil, a C-STORE call will produce an error response.
	CStore CStoreCallback

	// TLSConfig, if non-nil, enables TLS on the connection. See
	// https://gist.github.com/michaljemala/d6f4e01c4834bf47a9c4 for an
	// example for creating a TLS config from x509 cert files.
	TLSConfig *tls.Config
}

// DefaultMaxPDUSize is the the PDU size advertized by go-netdicom.
const DefaultMaxPDUSize = 4 << 20

// ServiceProvider encapsulates the state for DICOM server (provider).
type ServiceProvider struct {
	params   ServiceProviderParams
	listener net.Listener
	// Label is a unique string used in log messages to identify this provider.
	label string
}

/** c-echo callback */

// CEchoCallback implements C-ECHO callback. It typically just returns
// dimse.Success.
type CEchoCallback func(conn ConnectionState) dimse.Status

func handleCEcho(
	params ServiceProviderParams,
	connState ConnectionState,
	c *dimse.CEchoRq, data []byte,
	cs *serviceCommandState) {

	status := dimse.Status{Status: dimse.StatusUnrecognizedOperation}

	if params.CEcho != nil {
		status = params.CEcho(connState)
	}

	logrus.Infof("dicom_server.ServiceProvider: Received E-ECHO: context: %+v, status: %+v", cs.context, status)

	resp := &dimse.CEchoRsp{
		MessageIDBeingRespondedTo: c.MessageID,
		CommandDataSetType:        dimse.CommandDataSetTypeNull,
		Status:                    status,
	}

	cs.sendMessage(resp, nil)
}

/** c-echo end */

/** c-find callback */

// CFindCallback 继承自 C-FIND handler,
// sopClassUID是要求的data的uid. (e.g. "1.2.840.10008.5.1.4.1.1.1.2")
// transferSyntaxUID是要求的data的编码 (e.g., "1.2.840.10008.1.2.1")
// 这些值是从请求包中提取出的
//
// 这个函数应该通过ch传入(stream)CFIndResult object
// 这个函数也许被阻挡？(block)
// 为了找到（report）一个匹配的dicom dataset, 这个函数应该传入channel一个非空Element filed进CFindResult
// 为了找到（report）多个匹配的dicom datasets, 这个函数必须在处理了所有请求后关闭channel
type CFindCallback func(
	conn ConnectionState,
	transferSyntaxUID string,
	sopClassUID string,
	filters []*dicom.Element,
	ch chan CFindResult)

func handleCFind(
	params ServiceProviderParams,
	connState ConnectionState,
	c *dimse.CFindRq,
	data []byte,
	cs *serviceCommandState) {
	if params.CFind == nil {
		cs.sendMessage(&dimse.CFindRsp{
			AffectedSOPClassUID:       c.AffectedSOPClassUID,
			MessageIDBeingRespondedTo: c.MessageID,
			CommandDataSetType:        dimse.CommandDataSetTypeNull,
			Status:                    dimse.Status{Status: dimse.StatusUnrecognizedOperation, ErrorComment: "没有找到C-Find的回调函数"},
		}, nil)

		return
	}

	elements, err := readElementsInBytes(data, cs.context.transferSyntaxUID)
	if err != nil {
		cs.sendMessage(&dimse.CFindRsp{
			AffectedSOPClassUID:       c.AffectedSOPClassUID,
			MessageIDBeingRespondedTo: c.MessageID,
			CommandDataSetType:        dimse.CommandDataSetTypeNull,
			Status:                    dimse.Status{Status: dimse.StatusUnrecognizedOperation, ErrorComment: err.Error()},
		}, nil)
		return
	}

	logrus.Infof("dicom.serviceProvider: C-FIND-RQ payload: %s", elementsString(elements))

	status := dimse.Status{Status: dimse.StatusSuccess}

	// TODO 这里最多128个链接？
	responseCh := make(chan CFindResult, 128)

	go func() {
		params.CFind(connState, cs.context.transferSyntaxUID, c.AffectedSOPClassUID, elements, responseCh)
	}()

	for resp := range responseCh {
		if resp.Err != nil {
			status = dimse.Status{
				Status:       dimse.CFindUnableToProcess,
				ErrorComment: resp.Err.Error(),
			}
			break
		}
		logrus.Infof("dicom.serviceProvider: C-FIND-RSP: %s", elementsString(resp.Elements))

		payload, err := writeElementsToBytes(resp.Elements, cs.context.transferSyntaxUID)

		if err != nil {
			logrus.Warnf("dicom.serviceProvider: C-FIND: encode error %v", err)

			status = dimse.Status{Status: dimse.CFindUnableToProcess, ErrorComment: err.Error()}
			break
		}

		cs.sendMessage(&dimse.CFindRsp{
			AffectedSOPClassUID:       c.AffectedSOPClassUID,
			MessageIDBeingRespondedTo: c.MessageID,
			CommandDataSetType:        dimse.CommandDataSetTypeNonNull,
			Status:                    dimse.Status{Status: dimse.StatusPending},
		}, payload)
	}

	cs.sendMessage(&dimse.CFindRsp{
		AffectedSOPClassUID:       c.AffectedSOPClassUID,
		MessageIDBeingRespondedTo: c.MessageID,
		CommandDataSetType:        dimse.CommandDataSetTypeNull,
		Status:                    status,
	}, nil)

	// 如果出错，就跑空responseCh channel
	for range responseCh {
	}
}

/** c-find end */

/** c-move callback */

// CMoveCallback 是C-MOVE和C-GET的callback
// sopClassUID是要求的data的uid. (e.g. "1.2.840.10008.5.1.4.1.1.1.2")
// transferSyntaxUID是要求的data的编码 (e.g., "1.2.840.10008.1.2.1")
// 这些值是从请求包中提取出的
//
// 此cb必须传输datasets或error入channel
// 此cb可能会阻塞其他方法
// 此cb必须在处理完全部datasets后关闭channel
type CMoveCallback func(
	conn ConnectionState,
	transferSyntaxUID string,
	sopClassUID string,
	filters []*dicom.Element,
	ch chan CMoveResult,
)

func handleCMove(
	params ServiceProviderParams,
	connState ConnectionState,
	c *dimse.CMoveRq,
	data []byte,
	cs *serviceCommandState) {
	sendError := func(err error) {
		cs.sendMessage(&dimse.CMoveRsp{
			AffectedSOPClassUID:       c.AffectedSOPClassUID,
			MessageIDBeingRespondedTo: c.MessageID,
			CommandDataSetType:        dimse.CommandDataSetTypeNull,
			Status:                    dimse.Status{Status: dimse.StatusUnrecognizedOperation, ErrorComment: err.Error()},
		}, nil)
	}

	if params.CMove == nil {
		cs.sendMessage(&dimse.CMoveRsp{
			AffectedSOPClassUID:       c.AffectedSOPClassUID,
			MessageIDBeingRespondedTo: c.MessageID,
			CommandDataSetType:        dimse.CommandDataSetTypeNull,
			Status:                    dimse.Status{Status: dimse.StatusUnrecognizedOperation, ErrorComment: "没有找到c-move的回调函数"},
		}, nil)

		return
	}

	remoteHostPort, ok := params.RemoteAEs[c.MoveDestination]

	if !ok {
		sendError(fmt.Errorf("C-MOVE destination %v not registered in the server", c.MoveDestination))
		logrus.Errorf("C-MOVE传进了没有在Server注册的remote AETitle %v, 请先将其添加进列表", c.MoveDestination)
		return
	}

	elements, err := readElementsInBytes(data, cs.context.transferSyntaxUID)
	if err != nil {
		sendError(err)
		return
	}

	logrus.Infof("dicom_server.serviceProvider: C-MOVE-RQ payload: %s", elementsString(elements))
	responseCh := make(chan CMoveResult, 128)

	go func() {
		params.CMove(connState, cs.context.transferSyntaxUID, c.AffectedSOPClassUID, elements, responseCh)
	}()

	status := dimse.Status{Status: dimse.StatusSuccess}

	var numSuccesses, numFailures uint16

	for resp := range responseCh {
		if resp.Err != nil {
			status = dimse.Status{
				// 出错了就是CFindUnableToProcess 错误码可能一样
				Status:       dimse.CFindUnableToProcess,
				ErrorComment: resp.Err.Error(),
			}
			break
		}

		logrus.Warnf("dicom_server.serviceProvider: C-MOVE: Sending %v to %v(%s)", resp.Path, c.MoveDestination, remoteHostPort)

		err := runCStoreOnNewAssociation(
			params.AETitle,
			c.MoveDestination,
			remoteHostPort,
			resp.DataSet,
		)
		if err != nil {
			logrus.Errorf("dicom_server.serviceProvider: C-MOVE: C-store of %v to %v(%v) 失败: %v",
				resp.Path, c.MoveDestination, remoteHostPort, err)
			numFailures++
		} else {
			numSuccesses++
		}

		cs.sendMessage(&dimse.CMoveRsp{
			AffectedSOPClassUID:            c.AffectedSOPClassUID,
			MessageIDBeingRespondedTo:      c.MessageID,
			CommandDataSetType:             dimse.CommandDataSetTypeNull,
			NumberOfRemainingSuboperations: uint16(resp.Remaining),
			NumberOfCompletedSuboperations: numSuccesses,
			NumberOfFailedSuboperations:    numFailures,
			Status:                         dimse.Status{Status: dimse.StatusPending},
		}, nil)

	}
	cs.sendMessage(&dimse.CMoveRsp{
		AffectedSOPClassUID:            c.AffectedSOPClassUID,
		MessageIDBeingRespondedTo:      c.MessageID,
		CommandDataSetType:             dimse.CommandDataSetTypeNull,
		NumberOfCompletedSuboperations: numSuccesses,
		NumberOfFailedSuboperations:    numFailures,
		Status:                         status}, nil)
	// 如果出错就跑完responses
	for range responseCh {
	}
}

// CMoveRusult 是通过CMove流式传输的对象
type CMoveResult struct {
	Remaining int            // 待发送的文件数量
	Err       error          //
	Path      string         // 被复制的dicom 文件路径，只在上报错误时使用
	DataSet   *dicom.DataSet // 文件内容
}

/** c-move end */

/** c-get */
func handleCGet(
	params ServiceProviderParams,
	connState ConnectionState,
	c *dimse.CGetRq, data []byte, cs *serviceCommandState) {
	sendError := func(err error) {
		cs.sendMessage(&dimse.CGetRsp{
			AffectedSOPClassUID:       c.AffectedSOPClassUID,
			MessageIDBeingRespondedTo: c.MessageID,
			CommandDataSetType:        dimse.CommandDataSetTypeNull,
			Status:                    dimse.Status{Status: dimse.StatusUnrecognizedOperation, ErrorComment: err.Error()},
		}, nil)
	}

	if params.CGet == nil {
		cs.sendMessage(&dimse.CGetRsp{
			AffectedSOPClassUID:       c.AffectedSOPClassUID,
			MessageIDBeingRespondedTo: c.MessageID,
			CommandDataSetType:        dimse.CommandDataSetTypeNull,
			Status:                    dimse.Status{Status: dimse.StatusUnrecognizedOperation, ErrorComment: "没有找到c-get的回调函数"},
		}, nil)
		return
	}

	elements, err := readElementsInBytes(data, cs.context.transferSyntaxUID)
	if err != nil {
		sendError(err)
	}

	logrus.Infof("dicom_server.serviceProvider: C-GET-RQ payload: %s", elementsString(elements))

	responseCh := make(chan CMoveResult, 128)

	go func() {
		params.CGet(connState, cs.context.transferSyntaxUID, c.AffectedSOPClassUID, elements, responseCh)
	}()

	status := dimse.Status{Status: dimse.StatusSuccess}

	var numSuccesses, numFailures uint16

	for resp := range responseCh {
		if resp.Err != nil {
			status = dimse.Status{
				Status:       dimse.CFindUnableToProcess,
				ErrorComment: resp.Err.Error(),
			}
			break
		}

		subCs, err := cs.disp.newCommand(cs.cm, cs.context /** 暂未使用*/)
		if err != nil {
			status = dimse.Status{
				Status:       dimse.CFindUnableToProcess,
				ErrorComment: err.Error(),
			}
			break
		}

		err = runCStoreOnAssociation(subCs.upcallCh, subCs.disp.downcallCh, subCs.cm, subCs.messageID, resp.DataSet)
		if err != nil {
			logrus.Errorf("dicom_server.serviceProvider: C-GET: C-store of %v 失败: %v", resp.Path, err)
			numFailures++
		} else {
			logrus.Infof("dicom_server.serviceProvider: C-GET: Send %v", resp.Path)
			numSuccesses++
		}

		cs.sendMessage(&dimse.CGetRsp{
			AffectedSOPClassUID:            c.AffectedSOPClassUID,
			MessageIDBeingRespondedTo:      c.MessageID,
			CommandDataSetType:             dimse.CommandDataSetTypeNull,
			NumberOfRemainingSuboperations: uint16(resp.Remaining),
			NumberOfCompletedSuboperations: numSuccesses,
			NumberOfFailedSuboperations:    numFailures,
			Status:                         dimse.Status{Status: dimse.StatusPending},
		}, nil)
	}

	cs.sendMessage(&dimse.CGetRsp{
		AffectedSOPClassUID:            c.AffectedSOPClassUID,
		MessageIDBeingRespondedTo:      c.MessageID,
		CommandDataSetType:             dimse.CommandDataSetTypeNull,
		NumberOfCompletedSuboperations: numSuccesses,
		NumberOfFailedSuboperations:    numFailures,
		Status:                         status}, nil)

	// 出错就跑完channel
	for range responseCh {
	}
}

/** c-get end */

/** c-store callback */

// CStoreCallback 是c-store请求的cb
// sopInstanceUID 是data的uid.
// sopClassUID 是需要的data type(e.g.,"1.2.840.10008.5.1.4.1.1.1.2")
// transferSyntaxUID 是data的编码 (e.g., "1.2.840.10008.1.2.1").
// 这些数据是从request包提取出来的
//
// "data" is the payload, i.e., a sequence of serialized dicom.DataElement
// objects in transferSyntaxUID.  "data" does not contain metadata elements
// (elements whose Tag.Group=2 -- e.g., TransferSyntaxUID and
// MediaStorageSOPClassUID), since they are stripped by the requster (two key
// metadata are passed as sop{Class,Instance)UID).
//
//这个函数应该保存从data中获取的编码spClass&spInstanceUID当DICOM header
// 应该在成功时返回dimse.Success0或在失败时返回一个CStoreStatus error code
type CStoreCallback func(
	conn ConnectionState,
	transferSyntaxUID string,
	sopClassUID string,
	sopInstanceUID string,
	data []byte) dimse.Status

func handleCStore(
	cb CStoreCallback,
	connState ConnectionState,
	c *dimse.CStoreRq,
	data []byte,
	cs *serviceCommandState) {
	status := dimse.Status{Status: dimse.StatusUnrecognizedOperation}

	if cb != nil {
		status = cb(
			connState,
			cs.context.transferSyntaxUID,
			c.AffectedSOPClassUID,
			c.AffectedSOPInstanceUID,
			data)
	}

	resp := &dimse.CStoreRsp{
		AffectedSOPClassUID:       c.AffectedSOPClassUID,
		MessageIDBeingRespondedTo: c.MessageID,
		CommandDataSetType:        dimse.CommandDataSetTypeNull,
		AffectedSOPInstanceUID:    c.AffectedSOPInstanceUID,
		Status:                    status,
	}

	cs.sendMessage(resp, nil)
}

/** c-store end */

func writeElementsToBytes(elements []*dicom.Element, transferSyntaxUID string) ([]byte, error) {

	dataEncoder := dicomio.NewBytesEncoderWithTransferSyntax(transferSyntaxUID)

	for _, elem := range elements {
		dicom.WriteElement(dataEncoder, elem)
	}

	if err := dataEncoder.Error(); err != nil {
		return nil, err
	}

	return dataEncoder.Bytes(), nil
}

func readElementsInBytes(data []byte, transferSyntaxUID string) ([]*dicom.Element, error) {

	decoder := dicomio.NewBytesDecoderWithTransferSyntax(data, transferSyntaxUID)

	var elements []*dicom.Element

	for !decoder.EOF() {
		elem := dicom.ReadElement(decoder, dicom.ReadOptions{})

		logrus.Warnf("dicom.serviceProvider: C-FIND: Read elem: %v, err %v", elem, decoder.Error())

		if decoder.Error() != nil {
			break
		}

		elements = append(elements, elem)
	}

	if decoder.Error() != nil {
		return nil, decoder.Error()
	}

	return elements, nil
}

func elementsString(elements []*dicom.Element) string {
	s := "["

	for i, elem := range elements {
		if i > 0 {
			s += ", "
		}

		s += elem.String()
	}

	return s + "]"
}

// runCStoreOnNewAssociation 用c-store发送 'ds' 到remoteHostPort。 在c-move中调用
func runCStoreOnNewAssociation(myAETitle, remoteAETitle, remoteHostPort string, ds *dicom.DataSet) error {
	su, err := NewServiceUser(ServiceUserParams{
		CalledAETitle:  remoteAETitle,
		CallingAETitle: myAETitle,
		SOPClasses:     sopclass.StorageClasses,
	})

	if err != nil {
		return err
	}

	defer su.Release()
	su.Connect(remoteHostPort)
	err = su.CStore(ds)
	logrus.Infof("dicom_server.serviceProvider: C-STORE subop done: %v", err)

	return err
}

func NewServiceProvider(params ServiceProviderParams, port string) (*ServiceProvider, error) {

	sp := &ServiceProvider{
		params: params,
		label:  newUID("sp"),
	}

	var err error

	if params.TLSConfig != nil {
		sp.listener, err = tls.Listen("tcp", port, params.TLSConfig)
	} else {
		sp.listener, err = net.Listen("tcp", port)
	}

	if err != nil {
		return nil, err
	}

	return sp, nil
}

func getConnState(conn net.Conn) (cs ConnectionState) {
	tlsConn, ok := conn.(*tls.Conn)
	if ok {
		cs.TLS = tlsConn.ConnectionState()
	}
	return
}

// RunProviderForConn 开启额外线程在 ”conn“上 运行 dicom server
// 这个函数会立即返回， "conn"会在后台被清理
func RunProviderForConn(conn net.Conn, params ServiceProviderParams) {

	upcallCh := make(chan upcallEvent, 128)
	label := newUID("sc")
	disp := newServiceDispatcher(label)

	// registerCallback 注册回调 加锁用
	disp.registerCallback(dimse.CommandFieldCEchoRq,
		func(msg dimse.Message, data []byte, cs *serviceCommandState) {
			handleCEcho(params, getConnState(conn), msg.(*dimse.CEchoRq), data, cs)
		})
	disp.registerCallback(dimse.CommandFieldCFindRq,
		func(msg dimse.Message, data []byte, cs *serviceCommandState) {
			handleCFind(params, getConnState(conn), msg.(*dimse.CFindRq), data, cs)
		})
	disp.registerCallback(dimse.CommandFieldCMoveRq,
		func(msg dimse.Message, data []byte, cs *serviceCommandState) {
			handleCMove(params, getConnState(conn), msg.(*dimse.CMoveRq), data, cs)
		})
	disp.registerCallback(dimse.CommandFieldCGetRq,
		func(msg dimse.Message, data []byte, cs *serviceCommandState) {
			handleCGet(params, getConnState(conn), msg.(*dimse.CGetRq), data, cs)
		})
	disp.registerCallback(dimse.CommandFieldCStoreRq,
		func(msg dimse.Message, data []byte, cs *serviceCommandState) {
			handleCStore(params.CStore, getConnState(conn), msg.(*dimse.CStoreRq), data, cs)
		})
	go runStateMachineForServiceProvider(conn, upcallCh, disp.downcallCh, label)
	for event := range upcallCh {
		disp.handleEvent(event)
	}

	logrus.Infof("dicom_service.serviceProvider(%s): Finished connection %p (remote %+v)", label, conn, conn.RemoteAddr())
	disp.close()
}

func (sp *ServiceProvider) Run() {

	for {
		conn, err := sp.listener.Accept()

		if err != nil {
			logrus.Warnf("orthanc-server.serviceProvider(%s): Accepted error: %v", sp.label, err)
			continue
		}

		logrus.Infof("orthanc-server.serviceProvider(%s): Accepted connection %p (remote: %+v)", sp.label, conn, conn.RemoteAddr())

		go func() { RunProviderForConn(conn, sp.params) }()
	}
}

func newUID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, atomic.AddInt32(&idSeq, 1))
}
