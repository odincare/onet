package onet

import (
	"fmt"
	"github.com/odincare/odicom"
	"github.com/odincare/odicom/dicomio"
	"github.com/odincare/odicom/dicomuid"
	"github.com/odincare/onet/pdu"

	"github.com/sirupsen/logrus"
)

// contextManager 管理一个 由contextID->corresponding abstract-syntax UID(也叫SOP) 的mappings
// UID的格式如“1.2.840.10008.5.1.4.1.1.1.2”
// UIDs 是全局&静态的，由https://www.dicomlibrary.com/dicom/sop/中定义
//
// 另外， 每次握手都会重新分配contextID.
// ContextID的值是 1/3/5/7...etc
// 每个新连接都会创建一个contextManager
type contextManager struct {
	label string // 用来诊断

	// 这两个图互相相反
	contextIDToAbstractSyntaxNameMap map[byte]*contextManagerEntry
	abstractSyntaxNameToContextIDMap map[string]*contextManagerEntry

	// 连接时另一方的信息，由A-ASSOCIATE-*pud收集
	peerMaxPDUSize int
	// 识别peer的UID，它应该是全局（global）唯一的
	peerImplementationClassUID string
	// Implementation version, 已废弃，格式没有标准
	peerImplementationVersionName string

	// ! tmpRequests 只在请求方被使用，它保存一份由A_ASSOCIATE_RQ PDU生成的
	// contextID -> presentationContext mapping
	// A_ASSOCIATE_AC PDU到达时，temRequests会与response PDU对应？
	// (Once an A_ASSOCIATE_AC PDU arrives, tmpRequests is matched against the response PDU)
	// 填充contextID -> {abstractSyntax, transferSyntax} mapping
	tmpRequests map[byte]*pdu.PresentationContextItem
}

// 创建一个空的contextManager
func newContextManager(label string) *contextManager {
	c := &contextManager{
		label:                            label,
		contextIDToAbstractSyntaxNameMap: make(map[byte]*contextManagerEntry),
		abstractSyntaxNameToContextIDMap: make(map[string]*contextManagerEntry),
		peerMaxPDUSize:                   16384, // 在Osirix & pynetdicom.也是这个词
		tmpRequests:                      make(map[byte]*pdu.PresentationContextItem),
	}
	return c
}

// Called by the user (client) to produce a list to be embedded in an
// A_REQUEST_RQ.Items. The PDU is sent when running as a service user (client).
// maxPDUSize is the maximum PDU size, in bytes, that the clients is willing to
// receive. maxPDUSize is encoded in one of the items.
func (m *contextManager) generateAssociateRequest(
	sopClassUIDs []string, transferSyntaxUIDs []string) []pdu.SubItem {
	items := []pdu.SubItem{
		&pdu.ApplicationContextItem{
			Name: pdu.DICOMApplicationContextItemName,
		}}
	var contextID byte = 1
	for _, sop := range sopClassUIDs {
		syntaxItems := []pdu.SubItem{
			&pdu.AbstractSyntaxSubItem{Name: sop},
		}
		for _, syntaxUID := range transferSyntaxUIDs {
			syntaxItems = append(syntaxItems, &pdu.TransferSyntaxSubItem{Name: syntaxUID})
		}
		item := &pdu.PresentationContextItem{
			Type:      pdu.ItemTypePresentationContextRequest,
			ContextID: contextID,
			Result:    0, // must be zero for request
			Items:     syntaxItems,
		}
		items = append(items, item)
		m.tmpRequests[contextID] = item
		contextID += 2 // must be odd.
	}
	items = append(items,
		&pdu.UserInformationItem{
			Items: []pdu.SubItem{
				&pdu.UserInformationMaximumLengthItem{uint32(DefaultMaxPDUSize)},
				&pdu.ImplementationClassUIDSubItem{dicom.GoDICOMImplementationClassUID},
				&pdu.ImplementationVersionNameSubItem{dicom.GoDICOMImplementationVersionName}}})

	return items
}

// Called when A_ASSOCIATE_RQ pdu arrives, on the provider side. Returns a list of items to be sent in
// the A_ASSOCIATE_AC pdu.
func (m *contextManager) onAssociateRequest(requestItems []pdu.SubItem) ([]pdu.SubItem, error) {
	responses := []pdu.SubItem{
		&pdu.ApplicationContextItem{
			Name: pdu.DICOMApplicationContextItemName,
		},
	}
	for _, requestItem := range requestItems {
		switch ri := requestItem.(type) {
		case *pdu.ApplicationContextItem:
			if ri.Name != pdu.DICOMApplicationContextItemName {
				logrus.Warnf("dicom_server.onAssociateRequest(%s): Found illegal applicationcontextname. Expect %v, found %v",
					m.label, ri.Name, pdu.DICOMApplicationContextItemName)
			}
		case *pdu.PresentationContextItem:
			var sopUID string
			var pickedTransferSyntaxUID string
			for _, subItem := range ri.Items {
				switch c := subItem.(type) {
				case *pdu.AbstractSyntaxSubItem:
					if sopUID != "" {
						return nil, fmt.Errorf("dicom.onAssociateRequest: Multiple AbstractSyntaxSubItem found in %v",
							ri.String())
					}
					sopUID = c.Name
				case *pdu.TransferSyntaxSubItem:
					// Just pick the first syntax UID proposed by the client.
					if pickedTransferSyntaxUID == "" {
						pickedTransferSyntaxUID = c.Name
					}
				default:
					return nil, fmt.Errorf("dicom.onAssociateRequest: Unknown subitem in PresentationContext: %s",
						subItem.String())
				}
			}
			if sopUID == "" || pickedTransferSyntaxUID == "" {
				return nil, fmt.Errorf("dicom.onAssociateRequest: SOP or transfersyntax not found in PresentationContext: %v",
					ri.String())
			}
			responses = append(responses, &pdu.PresentationContextItem{
				Type:      pdu.ItemTypePresentationContextResponse,
				ContextID: ri.ContextID,
				Result:    0, // accepted
				Items:     []pdu.SubItem{&pdu.TransferSyntaxSubItem{Name: pickedTransferSyntaxUID}}})
			logrus.Infof("dicom.onAssociateRequest(%s): Provider(%p): addmapping %v %v %v",
				m.label, m, sopUID, pickedTransferSyntaxUID, ri.ContextID)
			// TODO 触发service provider的callback而不是随便接受sopclass
			addContextMapping(m, sopUID, pickedTransferSyntaxUID, ri.ContextID, pdu.PresentationContextAccepted)
		case *pdu.UserInformationItem:
			for _, subItem := range ri.Items {
				switch c := subItem.(type) {
				case *pdu.UserInformationMaximumLengthItem:
					m.peerMaxPDUSize = int(c.MaximumLengthReceived)
				case *pdu.ImplementationClassUIDSubItem:
					m.peerImplementationClassUID = c.Name
				case *pdu.ImplementationVersionNameSubItem:
					m.peerImplementationVersionName = c.Name

				}
			}
		}
	}
	responses = append(responses,
		&pdu.UserInformationItem{
			Items: []pdu.SubItem{&pdu.UserInformationMaximumLengthItem{MaximumLengthReceived: uint32(DefaultMaxPDUSize)}}})
	logrus.Infof("dicom_server.onAssociateRequest(%s): Received associate request, #contexts:%v, maxPDU:%v, implclass:%v, version:%v",
		m.label, len(m.contextIDToAbstractSyntaxNameMap),
		m.peerMaxPDUSize, m.peerImplementationClassUID, m.peerImplementationVersionName)
	return responses, nil
}

// Called by the user (client) to when A_ASSOCIATE_AC PDU arrives from the provider.
func (m *contextManager) onAssociateResponse(responses []pdu.SubItem) error {
	for _, responseItem := range responses {
		switch ri := responseItem.(type) {
		case *pdu.PresentationContextItem:
			var pickedTransferSyntaxUID string
			for _, subItem := range ri.Items {
				switch c := subItem.(type) {
				case *pdu.TransferSyntaxSubItem:
					// Just pick the first syntax UID proposed by the client.
					if pickedTransferSyntaxUID == "" {
						pickedTransferSyntaxUID = c.Name
					} else {
						return fmt.Errorf("multiple syntax UIDs returned in A_ASSOCIATE_AC: %v", ri.String())
					}
				default:
					return fmt.Errorf("unknown subitem %s in PresentationContext: %s", subItem.String(), ri.String())
				}
			}
			request, ok := m.tmpRequests[ri.ContextID]
			if !ok {
				return fmt.Errorf("unknown context ID %d for A_ASSOCIATE_AC: %v",
					ri.ContextID,
					ri.String())
			}
			found := false
			var sopUID string
			for _, subItem := range request.Items {
				switch c := subItem.(type) {
				case *pdu.AbstractSyntaxSubItem:
					sopUID = c.Name
				case *pdu.TransferSyntaxSubItem:
					if c.Name == pickedTransferSyntaxUID {
						found = true
						break
					}
				}
			}
			if sopUID == "" {
				return fmt.Errorf("dicom.onAssociateResponse(%s): The A-ASSOCIATE request lacks the abstract syntax item for tag %v (this shouldn't happen)", m.label, ri.ContextID)
			}
			if ri.Result != pdu.PresentationContextAccepted {
				logrus.Infof("dicom_server.onAssociateResponse(%s): Abstract syntax %v, transfer syntax %v was rejected by the server: %s", m.label, dicomuid.UIDString(sopUID), dicomuid.UIDString(pickedTransferSyntaxUID), ri.Result.String())
			}
			if !found {
				// Generally, we expect the server to pick a
				// transfer syntax that's in the A-ASSOCIATE-RQ
				// list, but it's not required to do so - e.g.,
				// Osirix SCP. That being the case, I'm not sure
				// the point of reporting the list in
				// A-ASSOCIATE-RQ, but that's only one of
				// DICOM's pointless complexities.
				logrus.Infof("dicom_server.onAssociateResponse(%s): The server picked TransferSyntaxUID '%s' for %s, which is not in the list proposed, %v",
					m.label,
					dicomuid.UIDString(pickedTransferSyntaxUID),
					dicomuid.UIDString(sopUID),
					request.Items)
			}
			addContextMapping(m, sopUID, pickedTransferSyntaxUID, ri.ContextID, ri.Result)
		case *pdu.UserInformationItem:
			for _, subItem := range ri.Items {
				switch c := subItem.(type) {
				case *pdu.UserInformationMaximumLengthItem:
					m.peerMaxPDUSize = int(c.MaximumLengthReceived)
				case *pdu.ImplementationClassUIDSubItem:
					m.peerImplementationClassUID = c.Name
				case *pdu.ImplementationVersionNameSubItem:
					m.peerImplementationVersionName = c.Name

				}
			}
		}
	}
	logrus.Infof("dicom.onAssociateResponse(%s): Received associate response, #contexts:%v, maxPDU:%v, implclass:%v, version:%v",
		m.label,
		len(m.contextIDToAbstractSyntaxNameMap),
		m.peerMaxPDUSize, m.peerImplementationClassUID, m.peerImplementationVersionName)
	return nil
}

// Add a mapping between a (global) UID and a (per-session) context ID.
func addContextMapping(
	m *contextManager,
	abstractSyntaxUID string,
	transferSyntaxUID string,
	contextID byte,
	result pdu.PresentationContextResult) {
	logrus.Infof("dicom.addContextMapping(%v): Map context %d -> %s, %s",
		m.label, contextID, dicomuid.UIDString(abstractSyntaxUID),
		dicomuid.UIDString(transferSyntaxUID))
	dicomio.DoAssert(result >= 0 && result <= 4, result)
	dicomio.DoAssert(contextID%2 == 1, contextID)
	if result == 0 {
		dicomio.DoAssert(abstractSyntaxUID != "", abstractSyntaxUID)
		dicomio.DoAssert(transferSyntaxUID != "", transferSyntaxUID)
	}
	e := &contextManagerEntry{
		abstractSyntaxUID: abstractSyntaxUID,
		transferSyntaxUID: transferSyntaxUID,
		contextID:         contextID,
		result:            result,
	}
	m.contextIDToAbstractSyntaxNameMap[contextID] = e
	m.abstractSyntaxNameToContextIDMap[abstractSyntaxUID] = e
}

func (m *contextManager) checkContextRejection(e *contextManagerEntry) error {
	if e.result != pdu.PresentationContextAccepted {
		return fmt.Errorf("dicom_server.checkContextRejection %v: Trying to use rejected context <%v, %v>: %s",
			m.label,
			dicomuid.UIDString(e.abstractSyntaxUID),
			dicomuid.UIDString(e.transferSyntaxUID),
			e.result.String())
	}
	return nil
}

// 将uid转为context id
func (m *contextManager) lookupByAbstractSyntaxUID(name string) (contextManagerEntry, error) {
	e, ok := m.abstractSyntaxNameToContextIDMap[name]
	if !ok {
		return contextManagerEntry{}, fmt.Errorf("dicom_server.checkContextRejection %v: Unknown syntax %s", m.label, dicomuid.UIDString(name))
	}
	err := m.checkContextRejection(e)
	if err != nil {
		return contextManagerEntry{}, err
	}
	return *e, nil
}

// 将contextID转为UID.
func (m *contextManager) lookupByContextID(contextID byte) (contextManagerEntry, error) {
	e, ok := m.contextIDToAbstractSyntaxNameMap[contextID]
	if !ok {
		return contextManagerEntry{}, fmt.Errorf("dicom_server.lookupByContextID %v: Unknown context ID %d", m.label, contextID)
	}
	err := m.checkContextRejection(e)
	if err != nil {
		return contextManagerEntry{}, err
	}
	return *e, nil
}
