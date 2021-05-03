package test

import (
	"testing"
	"time"

	"github.com/binhgo/go-sdk/sdk"
)

func TestGetPartitionNameDay(t *testing.T) {

	pModel := sdk.DBPartitionedModel{
		ColName:       "user",
		PartitionType: sdk.PartitionTypes.Day,
	}

	value := pModel.GetNameFromValue(time.Now())
	if value != "user_2019_03_29" {
		t.Errorf("Get name = %s , want : user_2019_03_29", value)
	}
}

func TestGetPartitionNameMonth(t *testing.T) {

	pModel := sdk.DBPartitionedModel{
		ColName:       "user",
		PartitionType: sdk.PartitionTypes.Month,
	}

	value := pModel.GetNameFromValue(time.Now())
	if value != "user_2019_03" {
		t.Errorf("Get name = %s , want : user_2019_03", value)
	}
}

func TestGetPartitionNameYear(t *testing.T) {

	pModel := sdk.DBPartitionedModel{
		ColName:       "user",
		PartitionType: sdk.PartitionTypes.Year,
	}

	value := pModel.GetNameFromValue(time.Now())
	if value != "user_2019" {
		t.Errorf("Get name = %s , want : user_2019", value)
	}
}

type User struct {
	Name string
}

func TestInitPartition(t *testing.T) {
	onInitCollection := func(session *sdk.DBSession, colName string, dbName string) (sdk.DBModel, error) {
		model := sdk.DBModel{
			ColName: colName,
			DBName:  dbName,
		}
		err := model.Init(session)
		return model, err
	}

	pModel := sdk.DBPartitionedModel{
		ColName:          "user",
		DbName:           "inside_partition_testing",
		OnInitCollection: onInitCollection,
	}

	var app = sdk.NewApp("Test App")
	// setup db client
	var db = app.SetupDBClient(sdk.DBConfiguration{
		Address:  []string{"103.20.150.227:27017"},
		Username: "admin",
		Password: "43219990009991234",
	})

	db.OnConnected(func(session *sdk.DBSession) error {
		pModel.SetDatabase(session, sdk.PartitionTypes.Year)

		return nil
	})

	app.Launch()
}
