package main

import (
	"Client/model"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
)

const SqlConnect = "user=golang_serv password=golang_serv dbname=WB_first sslmode=disable"

func SetDataToDB(data model.Order) error {
	db, err := sql.Open("postgres", SqlConnect)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Println(fmt.Errorf("transaction error %s", err))
	}
	defer tx.Rollback()

	_, err = tx.Exec("select adddeliverydata($1,$2,$3,$4,$5,$6,$7)", data.Delivery.Name,
		data.Delivery.Phone, data.Delivery.Zip, data.Delivery.City, data.Delivery.Address, data.Delivery.Region,
		data.Delivery.Email)
	if err != nil {
		return fmt.Errorf("wrong data: %s", err)
	}
	//fmt.Println(result.RowsAffected())

	_, err = tx.Exec("select addpaymentdata($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", data.Payment.Transaction,
		data.Payment.RequestId, data.Payment.Currency, data.Payment.Provider, data.Payment.Amount,
		data.Payment.PaymentDt, data.Payment.Bank, data.Payment.DeliveryCost, data.Payment.GoodsTotal,
		data.Payment.CustomFee)
	if err != nil {
		return fmt.Errorf("wrong data: %s", err)
	}
	//fmt.Println(result.RowsAffected())

	_, err = tx.Exec("select addorderdata($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", data.OrderUid,
		data.TrackNumber, data.Entry, data.Payment.Transaction, data.Locale, data.InternalSignature,
		data.CustomerId, data.DeliveryService, data.Shardkey, data.SmId, data.DateCreated, data.OofShard)
	if err != nil {
		return fmt.Errorf("wrong data: %s", err)
	}
	//fmt.Println(result.RowsAffected())

	for i, _ := range data.Items {
		_, err = tx.Exec("select additemdata($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", data.OrderUid,
			data.Items[i].ChrtId, data.Items[i].TrackNumber, data.Items[i].Price, data.Items[i].Rid, data.Items[i].Name,
			data.Items[i].Sale, data.Items[i].Size, data.Items[i].TotalPrice, data.Items[i].NmId,
			data.Items[i].Brand, data.Items[i].Status)
		if err != nil {
			return fmt.Errorf("wrong data: %s", err)
		}
		//fmt.Println(result.RowsAffected())
	}

	return tx.Commit()
}

func GetDataFromDB(cache *model.Cashe) error {
	db, err := sql.Open("postgres", SqlConnect)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	orderRows, err := db.Query(`select * from "order"`)
	if err != nil {
		return fmt.Errorf("error getting rows from orders: %s", err)
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
			return fmt.Errorf("error reading order from db: %s", err)
		}

		deliveryRows, err := db.Query("select * from delivery where delivery.id = $1", deliveryId)
		if err != nil {
			return fmt.Errorf("error getting rows from delivery: %s", err)
		}

		deliveryRows.Next()
		err = deliveryRows.Scan(&data.Delivery.Phone, &data.Delivery.Zip, &data.Delivery.City, &data.Delivery.Address,
			&data.Delivery.Region, &data.Delivery.Email, &data.Delivery.Name, &deliveryId)
		if err != nil {
			return fmt.Errorf("error reading delivery from db: %s", err)
		}
		deliveryRows.Close()

		paymentRows, err := db.Query("select * from payment where payment.transaction = $1", paymentId)
		if err != nil {
			return fmt.Errorf("error getting rows from payment: %s", err)
		}

		paymentRows.Next()
		err = paymentRows.Scan(&data.Payment.Transaction, &data.Payment.RequestId, &data.Payment.Currency,
			&data.Payment.Provider, &data.Payment.Amount, &data.Payment.PaymentDt, &data.Payment.Bank,
			&data.Payment.DeliveryCost, &data.Payment.GoodsTotal, &data.Payment.CustomFee)
		if err != nil {
			return fmt.Errorf("error reading payment from db: %s", err)
		}
		paymentRows.Close()

		itemsRows, err := db.Query("select chrt_id, track_number, price, rid, name, sale, size,total_price, "+
			"nm_id, brand, status from items where items.order_uid = $1", data.OrderUid)
		if err != nil {
			return fmt.Errorf("error getting rows from items: %s", err)
		}
		data.Items = []model.Items{}
		for itemsRows.Next() {
			item := model.Items{}
			err = itemsRows.Scan(&item.ChrtId, &item.TrackNumber, &item.Price, &item.Rid, &item.Name,
				&item.Sale, &item.Size, &item.TotalPrice, &item.NmId, &item.Brand, &item.Status)
			if err != nil {
				return fmt.Errorf("error reading item from db: %s", err)
			}
			data.Items = append(data.Items, item)
		}
		itemsRows.Close()
		fmt.Println(data.OrderUid)
		cache.Lock()
		cache.Memory[data.OrderUid] = data
		cache.Unlock()
	}
	return nil
}
