package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DbConf struct {
	Dsn     string
	MaxIdle int
}

var (
	DbConn       *sql.DB
	Univercities = make(map[string]int)
	dbConf       DbConf //数据库配置文件
	// dbConnMap map[string]*sql.DB
	// dbLock   sync.RWMutex
	wLock sync.RWMutex
	wg    sync.WaitGroup
	j     int = 0
)

var NumSeg = map[string][]int{
	"yd": []int{134, 135, 136, 137, 138, 139, 147, 150, 151, 152, 157, 158, 159, 178, 182, 183, 184, 187, 188, 198},
	"lt": []int{130, 131, 132, 155, 156, 145, 176, 185, 186, 166},
	"dx": []int{133, 149, 153, 173, 177, 180, 181, 189, 199},
}

const (
	writeFile = "output.txt"
)

func main() {
	//数据库配置初始化;dbname=crm;port=3306
	dbConf.Dsn = ""
	dbConf.MaxIdle = 5

	//数据库连接
	var err error
	DbConn, err = makeDbConn()
	if err != nil {
		log.Fatalln("db connect error:", err.Error())
	}

	//加载成都市区所有高校
	fileName := "univercity_cd.txt"
	loadUnivercitiesCd(fileName)

	n := len(Univercities)
	if n == 0 {
		log.Fatalln("load failed：", fileName, err.Error())
	}

	start := time.Now()
	//计算协程数量
	wg.Add(n)
	log.Println(n)
	for uniName, _ := range Univercities {
		go statisticData(uniName)
	}
	wg.Wait()

	end := time.Now()
	dealTime := end.Sub(start)
	fmt.Printf("处理共计用时: %s\n", dealTime)
	fmt.Printf(" %d所大学没有数据\n", j)

}

func loadUnivercitiesCd(file string) {
	handler, err := os.Open(file)
	if err != nil {
		log.Fatalln("加载univercity_cd.txt错误:%s", err)
	}

	reader := bufio.NewReader(handler)

	var univercity string
	for {
		size, _ := fmt.Fscanln(reader, &univercity)
		if size == 0 {
			break
		}
		Univercities[univercity] = 1
	}
	log.Println("加载成功:", file)
}

func statisticData(uniName string) {
	defer wg.Done()

	//获取geohash
	geohashes := getGeohashByKeyword(uniName)
	if len(geohashes) == 0 {
		fmt.Println("===============================")
		j++
		return
	}

	countMap, err := getPeopleNumBySeg(geohashes)
	if err != nil {
		log.Fatalln("分号段人数统计错误：", err.Error())
	}

	var content string
	for seg, count := range countMap {
		// content += uniName + "\t" + seg + "\t" + strconv.Itoa(count) + "\t" + strings.Join(geohashes, ",") + "\r\n"
		content += uniName + "\t" + seg + "\t" + strconv.Itoa(count) + "\r\n"
	}

	//纪录 人数， 写入文件
	_, err = Output(content)
	if err != nil {
		log.Fatalln("写出文件错误:", err.Error())
	}

}

//通过geohash 及 分号段 进行人数统计
func getPeopleNumBySeg(geohashes []string) (res map[string]int, err error) {
	res = make(map[string]int)
	for _, geohash := range geohashes {
		for key, nums := range NumSeg {
			var sql string
			var count int

			numSegSql := dealArrToSqlStr(nums)
			sql = "select count(distinct user_phone) as count from user_geo where geohash = ? and (" + numSegSql + ") "

			err := DbConn.QueryRow(sql, geohash).Scan(&count)
			if err != nil {
				return res, err
			}
			res[key] += count
		}
	}

	return res, err
}

func getGeohashByKeyword(keyword string) (result []string) {
	sql := "select geohash from location_geo_detail where description like '%" + keyword + "%'"
	rows, _ := DbConn.Query(sql)
	log.Println(keyword)

	for rows.Next() {
		var geohash string
		if e := rows.Scan(&geohash); e != nil {
			log.Println("[ERROR] get row fail", e)
		}

		result = append(result, geohash)
	}
	return result

}

func makeDbConn() (*sql.DB, error) {
	conn, err := sql.Open("mysql", dbConf.Dsn)
	if err != nil {
		return nil, err
	}

	conn.SetMaxIdleConns(dbConf.MaxIdle)
	err = conn.Ping()

	return conn, err
}

func dealArrToSqlStr(numbers []int) (str string) {
	var tmp []string
	for _, num := range numbers {
		tmp = append(tmp, " user_phone like '"+strconv.Itoa(num)+"%' ")
	}

	return strings.Join(tmp, " or ")
}

/**
 * 判断文件是否存在  存在返回 true 不存在返回false
 */
func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

//写数据到文件
func Output(content string) (n int, err error) {
	var f *os.File
	if checkFileIsExist(writeFile) { //如果文件存在
		f, err = os.OpenFile(writeFile, os.O_APPEND|os.O_WRONLY, 0666) //打开文件
	} else {
		f, err = os.Create(writeFile) //创建文件
		log.Println("文件不存在")
	}
	defer f.Close()

	wLock.Lock()
	defer wLock.Unlock()

	log.Println(content)

	n, err = f.WriteString(content)
	f.Sync()

	// w := bufio.NewWriter(f)
	// n, err = w.WriteString(content)
	// w.Flush()

	return n, err
}
