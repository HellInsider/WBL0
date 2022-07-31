package main

import (
	"Client/model"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"os"
	"testing"
)

func TestSetDataToDB(t *testing.T) {
	var test model.Order
	file, _ := os.Open("test.json")
	defer file.Close()
	data, _ := ioutil.ReadAll(file)
	json.Unmarshal(data, &test)
	db, err := sql.Open("postgres", SqlConnect)
	if err != nil {
		t.Error("connection error: ")
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Error("start transaction error")
	}
	defer tx.Rollback()

	result, err := tx.Exec("select adddeliverydata($1,$2,$3,$4,$5,$6,$7)", test.Delivery.Name,
		test.Delivery.Phone, test.Delivery.Zip, test.Delivery.City, test.Delivery.Address, test.Delivery.Region,
		test.Delivery.Email)
	if err != nil {
		t.Error("delivery data error")
	}
	fmt.Println(result.RowsAffected())

	result, err = tx.Exec("select addpaymentdata($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", test.Payment.Transaction,
		test.Payment.RequestId, test.Payment.Currency, test.Payment.Provider, test.Payment.Amount,
		test.Payment.PaymentDt, test.Payment.Bank, test.Payment.DeliveryCost, test.Payment.GoodsTotal,
		test.Payment.CustomFee)
	if err != nil {
		t.Error("payment data error")
	}
	fmt.Println(result.RowsAffected())

	result, err = tx.Exec("select addorderdata($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", test.OrderUid,
		test.TrackNumber, test.Entry, test.Payment.Transaction, test.Locale, test.InternalSignature,
		test.CustomerId, test.DeliveryService, test.Shardkey, test.SmId, test.DateCreated, test.OofShard)
	if err != nil {
		t.Error("order data error")
	}
	fmt.Println(result.RowsAffected())

	for i, _ := range test.Items {
		result, err = tx.Exec("select additemdata($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", test.OrderUid,
			test.Items[i].ChrtId, test.Items[i].TrackNumber, test.Items[i].Price, test.Items[i].Rid, test.Items[i].Name,
			test.Items[i].Sale, test.Items[i].Size, test.Items[i].TotalPrice, test.Items[i].NmId,
			test.Items[i].Brand, test.Items[i].Status)
		if err != nil {
			t.Error("items data error")
		}

	}
	err = tx.Commit()
	if err != nil {
		t.Error("close transaction error")
	}
	return

}

func TestGetDataFromDB(t *testing.T) {
	db, err := sql.Open("postgres", SqlConnect)
	if err != nil {
		t.Error("connection error")
	}
	defer db.Close()

	orderRows, err := db.Query(`select * from "order"`)
	if err != nil {
		t.Error("taking order rows error")
	}
	defer orderRows.Close()

	var data model.Order
	var paymentId string
	var deliveryId string
	for orderRows.Next() {
		err = orderRows.Scan(&data.OrderUid, &data.TrackNumber, &data.Entry, &deliveryId, &paymentId, &data.Locale,
			&data.InternalSignature, &data.CustomerId, &data.DeliveryService, &data.Shardkey, &data.SmId, &data.DateCreated,
			&data.OofShard)
		if err != nil {
			t.Error("order data error")
		}

		deliveryRows, err := db.Query("select * from delivery where delivery.id = $1", deliveryId)
		if err != nil {
			t.Error("error getting rows from delivery")
		}

		deliveryRows.Next()
		err = deliveryRows.Scan(&data.Delivery.Phone, &data.Delivery.Zip, &data.Delivery.City, &data.Delivery.Address,
			&data.Delivery.Region, &data.Delivery.Email, &data.Delivery.Name, &deliveryId)
		if err != nil {
			t.Error("error reading delivery from db")
		}
		err = deliveryRows.Close()
		if err != nil {
			t.Error("close delivery error")
		}

		paymentRows, err := db.Query("select * from payment where payment.transaction = $1", paymentId)
		if err != nil {
			t.Error("open payment rows error")
		}

		paymentRows.Next()
		err = paymentRows.Scan(&data.Payment.Transaction, &data.Payment.RequestId, &data.Payment.Currency,
			&data.Payment.Provider, &data.Payment.Amount, &data.Payment.PaymentDt, &data.Payment.Bank,
			&data.Payment.DeliveryCost, &data.Payment.GoodsTotal, &data.Payment.CustomFee)
		if err != nil {
			t.Error("error reading payment from db")
		}
		err = paymentRows.Close()
		if err != nil {
			t.Error("close payment error")
		}

		itemsRows, err := db.Query("select chrt_id, track_number, price, rid, name, sale, size,total_price, "+
			"nm_id, brand, status from items where items.order_uid = $1", data.OrderUid)
		if err != nil {
			t.Error("error getting rows from items")
		}
		data.Items = []model.Items{}
		for itemsRows.Next() {
			item := model.Items{}
			err = itemsRows.Scan(&item.ChrtId, &item.TrackNumber, &item.Price, &item.Rid, &item.Name,
				&item.Sale, &item.Size, &item.TotalPrice, &item.NmId, &item.Brand, &item.Status)
			if err != nil {
				t.Error("error reading item from db")
			}
			data.Items = append(data.Items, item)
		}
		err = itemsRows.Close()
		if err != nil {
			t.Error("close items error")
		}
		fmt.Println(data.OrderUid)
	}
	return
}
