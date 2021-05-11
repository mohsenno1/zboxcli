package service

import (
	"fmt"
	"github.com/0chain/gosdk/core/common"
	"github.com/0chain/gosdk/zboxcore/fileref"
	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/0chain/zboxcli/model"
	"log"
	"sync"
)

type UploadService struct {
	UploadConfig *model.UploadConfig
}

func NewUploadService(UpConfig *model.UploadConfig) *UploadService {
	return &UploadService{UploadConfig: UpConfig}
}

func (u *UploadService) UploadToDStorage() error {
	log.Printf("uploading: from localdir '%s' to remote '%s'", u.UploadConfig.LocalTempFilePath, u.UploadConfig.RemoteFilePath)
	allocationObj, err := sdk.GetAllocation(u.UploadConfig.AllocationID)
	if err != nil {
		return fmt.Errorf("error fetching the allocation. %s", err.Error())
	}

	wg := &sync.WaitGroup{}
	statusBar := &StatusBar{wg: wg}
	wg.Add(1)

	var attrs fileref.Attributes
	if u.UploadConfig.WhoPays != "" {
		var wp common.WhoPays
		if err = wp.Parse(u.UploadConfig.WhoPays); err != nil {
			return fmt.Errorf("error parssing who-pays value. %s", err.Error())
		}
		attrs.WhoPaysForReads = wp // set given value
	}
	if u.UploadConfig.Encrypt {
		err = allocationObj.EncryptAndUploadFile(u.UploadConfig.LocalTempFilePath, u.UploadConfig.RemoteFilePath, attrs, statusBar)
	} else {
		err = allocationObj.UploadFile(u.UploadConfig.LocalTempFilePath, u.UploadConfig.RemoteFilePath, attrs, statusBar)
	}

	if err != nil {
		return fmt.Errorf("upload failed. %s", err.Error())
	}
	wg.Wait()
	if !statusBar.success {
		log.Println("upload failed to complete.")
		return fmt.Errorf("upload failed. statusbar. success : %v", statusBar.success)
	}

	if u.UploadConfig.Commit {
		statusBar.wg.Add(1)
		err = commitMetaTxn(u.UploadConfig.RemoteFilePath, "Upload", "", "", allocationObj, nil, statusBar)
		statusBar.wg.Wait()
	}

	return nil
}

func commitMetaTxn(path, crudOp, authTicket, lookupHash string, a *sdk.Allocation, fileMeta *sdk.ConsolidatedFileMeta, status *StatusBar) error {
	err := a.CommitMetaTransaction(path, crudOp, authTicket, lookupHash, fileMeta, status)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
