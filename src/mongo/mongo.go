package mongo

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"os"
	"time"
)

var (
	session  *mgo.Session
	mongoUrl = "127.0.0.1:27017/deployDB"
	logColl  = "logs"
)

func getSession() *mgo.Session {
	if session == nil {
		var err error
		session, err = mgo.DialWithTimeout(mongoUrl, 3*time.Second)
		if err != nil {
			panic(err)
		}
	}
	return session.Clone()
}

func SaveLog(docs ...interface{}) {
	s := getSession()
	defer s.Close()
	if err := s.DB("").C(logColl).Insert(docs...); err != nil {
		fmt.Fprintf(os.Stderr, "Save to mongodb error: %s\n", err.Error())
	}
}
