package mongo

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"os"
	"time"
	"config"
)

var (
	session  *mgo.Session
	mongoUrl = "127.0.0.1:27017"
)

func GetSession() *mgo.Session {
	if session == nil {
		var err error
		session, err = mgo.DialWithTimeout(mongoUrl, 1*time.Minute)
		if err != nil {
			panic(err)
		}
	}
	return session.Clone()
}

func SaveLog(docs ...interface{}) {
	s := GetSession()
	defer s.Close()
	if err := s.DB(config.LogDB).C(config.LogColl).Insert(docs...); err != nil {
		fmt.Fprintf(os.Stderr, "Save to mongodb error: %s\n", err.Error())
	}
}

func Upsert(db, coll string, selector, update interface{}) {
	s := GetSession()
	defer s.Close()
	_, err := s.DB(db).C(coll).Upsert(selector, update)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Insert to mongodb error: %s\n", err.Error())
	}
}

func Remove(db, coll string, selector interface{}) {
	s := GetSession()
	defer s.Close()
	_, err := s.DB(db).C(coll).RemoveAll(selector)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Remove data error: %s\n", err.Error())
	}
}

func AggregateAll(db, coll string, query, result interface{}) error {
	s := GetSession()
	defer s.Close()
	return s.DB(db).C(coll).Pipe(query).All(result)
}


