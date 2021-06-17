package onet

import (
	"fmt"
	"github.com/odincare/odicom"
	"github.com/odincare/odicom/dicomio"
	"github.com/odincare/odicom/dicomtag"
	"github.com/odincare/odicom/dicomuid"
	"github.com/odincare/onet/dimse"

	"github.com/sirupsen/logrus"
)

// 一个
// C-{STORE,GET,MOVE}的辅助功能，通过一个已经建立好的连接使用c-store发送一个dataset
func runCStoreOnAssociation(upcallCh chan upcallEvent, downcallCh chan stateEvent,
	cm *contextManager,
	messageID dimse.MessageID,
	ds *dicom.DataSet) error {
	var getElement = func(tag dicomtag.Tag) (string, error) {
		elem, err := ds.FindElementByTag(tag)
		if err != nil {
			return "", fmt.Errorf("dicom_server.cstore: data lacks %s: %v", tag.String(), err)
		}
		s, err := elem.GetString()
		if err != nil {
			return "", err
		}
		return s, nil
	}
	sopInstanceUID, err := getElement(dicomtag.MediaStorageSOPInstanceUID)
	if err != nil {
		return fmt.Errorf("dicom_server.cstore: data lacks SOPInstanceUID: %v", err)
	}
	sopClassUID, err := getElement(dicomtag.MediaStorageSOPClassUID)
	if err != nil {
		return fmt.Errorf("dicom_server.cstore: data lacks MediaStorageSOPClassUID: %v", err)
	}
	logrus.Infof("dicom_server.cstore(%s): DICOM abstractsyntax: %s, sopinstance: %s", cm.label, dicomuid.UIDString(sopClassUID), sopInstanceUID)
	context, err := cm.lookupByAbstractSyntaxUID(sopClassUID)
	if err != nil {
		logrus.Errorf("dicom_server.cstore(%s): sop class %v not found in context %v", cm.label, sopClassUID, err)
		return err
	}
	logrus.Infof("dicom_server.cstore(%s): using transfersyntax %s to send sop class %s, instance %s",
		cm.label,
		dicomuid.UIDString(context.transferSyntaxUID),
		dicomuid.UIDString(sopClassUID),
		sopInstanceUID)
	bodyEncoder := dicomio.NewBytesEncoderWithTransferSyntax(context.transferSyntaxUID)
	for _, elem := range ds.Elements {
		if elem.Tag.Group == dicomtag.MetadataGroup {
			continue
		}
		dicom.WriteElement(bodyEncoder, elem)
	}
	if err := bodyEncoder.Error(); err != nil {
		logrus.Errorf("dicom_server.cstore(%s): body encoder failed: %v", cm.label, err)
		return err
	}
	downcallCh <- stateEvent{
		event: evt09,
		dimsePayload: &stateEventDIMSEPayload{
			abstractSyntaxName: sopClassUID,
			command: &dimse.CStoreRq{
				AffectedSOPClassUID:    sopClassUID,
				MessageID:              messageID,
				CommandDataSetType:     dimse.CommandDataSetTypeNonNull,
				AffectedSOPInstanceUID: sopInstanceUID,
			},
			data: bodyEncoder.Bytes(),
		},
	}
	for {
		logrus.Infof("dicom_server.cstore(%s): Start reading resp w/ messageID:%v", cm.label, messageID)
		event, ok := <-upcallCh
		if !ok {
			return fmt.Errorf("dicom_server.cstore(%s): Connection closed while waiting for C-STORE response", cm.label)
		}
		logrus.Infof("dicom_server.cstore(%s): resp event: %v", cm.label, event.command)
		dicomio.DoAssert(event.eventType == upcallEventData)
		dicomio.DoAssert(event.command != nil)
		resp, ok := event.command.(*dimse.CStoreRsp)
		dicomio.DoAssert(ok)
		if resp.Status.Status != 0 {
			return fmt.Errorf("dicom_server.cstore(%s): failed: %v", cm.label, resp.String())
		}
		return nil
	}
}
