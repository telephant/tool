/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "validator/routeguide"
)

const (
	address        = "localhost:10009"
	defaultContent = "云喇叭快递小管家大师的出生地措施"
)

func main() {
	start := time.Now()
	fileName := "./text.txt"
	file, err := os.OpenFile(fileName, os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Open file error!", err)
		return
	}
	defer file.Close()

	buf := bufio.NewReader(file)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			log.Fatalln("read line err:", err)
		}

		var status int
		var content string

		arr := strings.Split(line, ",")
		length := len(arr)
		if length != 3 {
			content = strings.Trim(strings.Join(arr[0:length-2], ","), "\"")
			status, _ = strconv.Atoi(strings.Trim(arr[length-2], "\""))
			log.Println("=======================")
			log.Println("status", status, content)
		} else {
			content = strings.Trim(arr[0], "\"")
			status, _ = strconv.Atoi(strings.Trim(arr[1], "\""))
		}

		// Set up a connection to the server.
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()
		c := pb.NewRouteGuideClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		r, err := c.SetTemplateStatus(ctx, &pb.NotifySetter{Content: content, MerchantId: 1, Status: int32(status)})
		if err != nil {
			log.Println(address)
			log.Fatalf("could not connect: %v", err)
		}
		log.Println(r.Suc)
		end := time.Now()
		dealTime := end.Sub(start)
		log.Println("处理共计用时: ", dealTime)
		time.Sleep(100 * time.Millisecond)
	}

}
