package analyze

import (
	"config"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"mongo"
	"os"
	"time"
)

type aggregateResult struct {
	ID    string `bson:"_id"`
	Count uint64 `bson:"count"`
}

func Analyze() {
	fmt.Printf("%s Statring analyze ...\n", time.Now().Format("2006-01-02 15:04:05"))
	getIPTotal()
	getPVTotal()
	getPVPreHour()
	getTCTop()
	dropOldData()
}

func getIPTotal() {
	var result []aggregateResult
	tmp := make(map[string]interface{})
	now := time.Now()
	today, _ := time.ParseInLocation("2006-01-02", now.Format("2006-01-02"), time.FixedZone("CST", 28800))
	yesterday := today.Add(-24 * time.Hour)
	selector := bson.M{"date": yesterday.Format("2006-01-02")}
	query := []bson.M{
		{"$match": bson.M{"date": bson.M{"$gte": yesterday, "$lt": today}}},
		{"$group": bson.M{"_id": bson.M{"ip": "$ip", "aid": "$aid"}}},
		{"$group": bson.M{"_id": "$_id.aid", "count": bson.M{"$sum": 1}}},
	}
	if err := mongo.AggregateAll(config.LogDB, config.LogColl, query, &result); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	for _, entry := range result {
		tmp[entry.ID] = entry.Count
	}
	tmp["date"] = yesterday.Format("2006-01-02")
	mongo.Upsert(config.LogDB, config.IPTotalColl, selector, tmp)
}

func getPVTotal() {
	var result []aggregateResult
	tmp := make(map[string]interface{})
	now := time.Now()
	today, _ := time.ParseInLocation("2006-01-02", now.Format("2006-01-02"), time.FixedZone("CST", 28800))
	yesterday := today.Add(-24 * time.Hour)
	selector := bson.M{"date": yesterday.Format("2006-01-02")}
	query := []bson.M{
		{"$match": bson.M{"date": bson.M{"$gte": yesterday, "$lt": today}}},
		{"$group": bson.M{"_id": "$aid", "count": bson.M{"$sum": 1}}},
	}
	if err := mongo.AggregateAll(config.LogDB, config.LogColl, query, &result); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	for _, entry := range result {
		tmp[entry.ID] = entry.Count
	}
	tmp["date"] = yesterday.Format("2006-01-02")
	mongo.Upsert(config.LogDB, config.PVTotalColl, selector, tmp)
}

func getPVPreHour() {
	var result []struct {
		ID struct {
			Aid  string
			Hour int
		} `bson:"_id"`
		Count uint64
	}
	tmp := make(map[string]interface{})
	now := time.Now()
	today, _ := time.ParseInLocation("2006-01-02", now.Format("2006-01-02"), time.FixedZone("CST", 28800))
	yesterday := today.Add(-24 * time.Hour)
	selector := bson.M{"date": yesterday.Format("2006-01-02")}
	query := []bson.M{
		{"$match": bson.M{"date": bson.M{"$gte": yesterday, "$lt": today}}},
		{"$group": bson.M{"_id": bson.M{"aid": "$aid", "hour": bson.M{"$hour": "$date"}}, "count": bson.M{"$sum": 1}}},
	}
	if err := mongo.AggregateAll(config.LogDB, config.LogColl, query, &result); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	// mongodb 存储UTC时间与CST相差8小时，所以查询结果+8
	for _, entry := range result {
		if _, ok := tmp[entry.ID.Aid]; !ok {
			tmp[entry.ID.Aid] = make([]uint64, 24)
		}
		index := entry.ID.Hour + 8
		if index >= 24 {
			index = entry.ID.Hour - 16
		}
		tmp[entry.ID.Aid].([]uint64)[index] = entry.Count
	}
	tmp["date"] = yesterday.Format("2006-01-02")
	mongo.Upsert(config.LogDB, config.PVPareHourColl, selector, tmp)
}

func getTCTop() {
	var result []struct {
		Path  string `bson:"_id"`
		Times uint64
		TCAvg float32 `bson:"tc_avg"`
	}
	tmp := make(map[string]interface{})
	now := time.Now()
	today, _ := time.ParseInLocation("2006-01-02", now.Format("2006-01-02"), time.FixedZone("CST", 28800))
	yesterday := today.Add(-24 * time.Hour)
	selector := bson.M{"date": yesterday.Format("2006-01-02")}
	query := []bson.M{
		{"$match": bson.M{"date": bson.M{"$gte": yesterday, "$lt": today}}},
		{"$group": bson.M{"_id": "$path", "times": bson.M{"$sum": 1}, "count": bson.M{"$sum": "$handletime"}}},
		{"$project": bson.M{"times": "$times", "tc_avg": bson.M{"$divide": []string{"$count", "$times"}}}},
		{"$sort": bson.M{"tc_avg": -1}},
		{"$limit": 20},
	}
	if err := mongo.AggregateAll(config.LogDB, config.LogColl, query, &result); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	tmp["date"] = yesterday.Format("2006-01-02")
	tmp["tctop"] = result
	mongo.Upsert(config.LogDB, config.TCTOPUrlColl, selector, tmp)
}

// 删除三天前的数据
func dropOldData() {
	now := time.Now()
	today, _ := time.ParseInLocation("2006-01-02", now.Format("2006-01-02"), time.FixedZone("CST", 28800))
	oldDay := today.Add(-72 * time.Hour)
	mongo.Remove(config.LogDB, config.LogColl, bson.M{"date": bson.M{"$lt": oldDay}})
}
