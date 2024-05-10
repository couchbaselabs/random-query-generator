package main

import (
	"fmt"
	"log"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"
	"github.com/go-faker/faker/v4"
	"flag"
	"github.com/couchbase/gocb/v2"
)

var wg sync.WaitGroup
var HAS_INCLUDE bool=true
var HAS_LEADING_KEY bool=true
var mutex sync.Mutex
var LEADING_KEY string
var errors=0
var success=0
var dataset_map = make(map[string]string)
var array_review_map = make(map[string]string)
var array_ratings_map = make(map[string]string)
var htype = []string{"Inn", "Hostel", "Place", "Center", "Hotel", "Motel", "Suites"}
var boolean = []string{"true", "false"}
var clauses = []string{"AND","OR"}
var array_fields string
var array_indexed_fields string

func randomAlphabet() byte {
	rand.Seed(time.Now().UnixNano())

	// ASCII values for lowercase alphabets are from 97 to 122
	var charset string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	// Convert ASCII value to a string representing the corresponding alphabet
	randomChar := charset[rand.Intn(len(charset))]
	return randomChar
}
func randomNumber(min int, max int) int {
	rand.Seed(time.Now().UnixNano())

	// Generate a random integer within the range
	randomInt := rand.Intn(max-min+1) + min
	return randomInt
}
func randomSign() string {
	rand.Seed(time.Now().UnixNano())
	charset:=[]string{"<",">","="}
	return charset[rand.Intn(len(charset))]
}
func genSelectBlock(dataset string) string{
	dataset_slice := []string{}
	switch dataset{
	case "hotel":
		dataset_map["country"] = "string"
		dataset_map["address"] = "string"
		dataset_map["free_parking"] = "bool"
		dataset_map["city"] = "string"
		dataset_map["url"] = "string"
		dataset_map["phone"] = "int"
		dataset_map["price"] = "int"
		dataset_map["avg_rating"] = "int"
		dataset_map["free_breakfast"] = "bool"
		dataset_map["name"] = "string"
		dataset_map["email"] = "string"
		dataset_map["type"] = "string"
		dataset_map["review"] = "array"
		//defining review array map
		array_review_map["date"] = "int"
		array_review_map["author"] = "string"
		array_review_map["ratings"] = "array"
		//defining ratings review map
		array_ratings_map["value"] = "int"
		array_ratings_map["cleaniness"] = "int"
		array_ratings_map["overall"] = "int"
		array_ratings_map["Check in / front desk"] = "int"
		array_ratings_map["rooms"] = "int"
		//dataset_slice = {"country","address","free_parking","city","url","phone","price","avg_rating","free_breakfast","name","email","htype"}
		dataset_slice = append(dataset_slice, "country","address","free_parking","city","url","phone","price","avg_rating","free_breakfast","name","email","htype")
	case "person":
		dataset_map["name"] = "string"
		dataset_map["age"] = "int"
		dataset_map["animals"] = "array"
		dataset_map["gender"] = "string"
		dataset_map["marital"] = "string"
		dataset_slice = append(dataset_slice, "name","age","animals","gender","marital")
	}
	key :=rand.Intn(2)
	var select_block string
	switch key{
	case 0:
		select_block = "*"
	case 1:
		select_block = fmt.Sprintf("%s", dataset_slice[rand.Intn(len(dataset_slice))])
	default:
		select_block = "*"

	}
	return select_block
}
func concatenateSentences(sentences []string, N int, clause []string) string {
	if N > len(sentences) {
		N = len(sentences)
	}
	var builder strings.Builder
	for i := 0; i < N; i++ {
		builder.WriteString(sentences[i])
		if i < N-1 {
		    clauses:=clause[rand.Intn(len(clause))]
			builder.WriteString(" " + clauses + " ")
		}
	}

	return builder.String()
}


func genWhereBlock(fields []string) string{
	for i,ele := range fields{
		fmt.Printf("array element %d is %s\n",i,ele)
		if strings.Contains(ele,"ARRAY") || strings.Contains(ele,"array") && array_fields!=""{
			array_fields = ele
			array_fields:=strings.Split(strings.Split(array_fields,"ARRAY")[1],".")
			for i,ele:= range array_fields{
				array_fields[i]=strings.Trim(ele," `")
			}
			fmt.Println("array string is ", array_fields)
			break
		}
	}
	
//Remove all backticks
	for i,ele:= range fields{
		fields[i]=strings.Trim(ele," `")
		fmt.Printf("fields %d is %s\n",i,fields[i])
	}
	//Check for INCLUDE keyword and set leading key
	for i,ele:= range fields{
		if strings.Contains(ele,"INCLUDE") || strings.Contains(ele,"include") && i>0{
			HAS_LEADING_KEY=false
			HAS_INCLUDE=true

			fields[i]=strings.Trim(strings.Split(ele," ")[0]," `")
			break
		}
	}
	
	// fmt.Print("fields are ", fields)
	if HAS_LEADING_KEY{
		LEADING_KEY=fields[0]
	}
	fmt.Printf("leading key %s\n",LEADING_KEY)
	key_fields := []string{}
	for i,ele :=range fields{
		// fmt.Printf("element %d is %s\n",i,ele)
		if _,ok := dataset_map[ele]; ok{
			fmt.Printf("element %d is %s\n",i,ele)
			key_fields = append(key_fields, ele)
		}
	}
	//constructing the fields for the where clause
	whereClauses:=getWhereClause(key_fields)
	if !HAS_LEADING_KEY{
		whereClauses[0],whereClauses[len(whereClauses)-1]=whereClauses[len(whereClauses)-1],whereClauses[0]
	}
	results:=[]string{}
	for i:=0;i<5;i++{
	    result := concatenateSentences(whereClauses, randomNumber(1,len(whereClauses)), clauses)
	   // fmt.Println(result)
	    results = append(results, string(result))
	}
	fmt.Println("results",results[rand.Intn(len(results))])

	return results[rand.Intn(len(results))]
}
func extractIndexDefinitionField(query string) []string {
	re:=regexp.MustCompile(`(?m)CREATE INDEX .*? ON .*?\((.*)\)`)
	elements := []string{}
	matches := re.FindStringSubmatch(query)
	if len(matches) > 1 {
		// if strings.Contains(matches[1],"ARRAY") || strings.Contains(matches[1],"array"){
		// 	array_re:=regexp.MustCompile(`(?m)ARRAY .*?\((.*)\) .*? .*? IN \x60(.*?)\x60`)
		// 	array_matches:=array_re.FindStringSubmatch(matches[1])
		// 	if len(array_matches)>1{
		// 		fmt.Println("array fields is : ",array_matches[1])
		// 		fmt.Println("array indexed fields is : ",array_matches[2])
		// 		array_fields=array_matches[1]
		// 		array_indexed_fields=array_matches[2]
				
		// 	}
		// }
		elements = strings.Split(matches[1], ",")
		fmt.Println("Elements are ",elements)
		return elements
	}else{
		return elements
	}
}
func genRandomSymbolandValue(field string) string{
	var symbol string
	fmt.Println("field type is ", field)
	switch dataset_map[field]{
	case "int":
		switch field{
		case "phone":
			symbol = fmt.Sprintf("%s %s %d",field,randomSign(),faker.Phonenumber)
		case "avg_rating":
			symbol = fmt.Sprintf("%s %s %d",field,randomSign(),randomNumber(1,5))
		case "age":
			symbol = fmt.Sprintf("%s %s %d",field,randomSign(),randomNumber(1,101))
		default:
			symbol = fmt.Sprintf("%s %s %d",field,randomSign(),randomNumber(1000,2000))
		
		}
	
	case "string":
		if field=="type"{
			symbol = fmt.Sprintf("%s = %s",field,htype[rand.Intn(len(htype))])
		}else{
			symbol = fmt.Sprintf("%s LIKE '%%%c%%'",field,randomAlphabet())
		}
	case "bool":
		symbol = fmt.Sprintf("%s = %s",field,boolean[rand.Intn(len(boolean))])
	default:
		symbol = fmt.Sprintf("%s %s %d",field,randomSign(),randomNumber(1000,2000))
	}
	return symbol
}

func getWhereClause(fields []string) []string{
	constructedWhereQueries:=[]string{}
	for _,field:= range fields{
		str := genRandomSymbolandValue(field)
		constructedWhereQueries = append(constructedWhereQueries, str)
	}
	fmt.Println("contsructed queries ", constructedWhereQueries)
	return constructedWhereQueries
}
func genKeySpace(query string)string{
	keyspace := strings.Split(strings.Split(query, " ")[4], "(")[0]
	keyspace = strings.Trim(keyspace, "`")
	return keyspace
}
func queryBuilder(query string, num_queries int, dataset string) []string{
	genrated_queries:=[]string{}
	for i:=0;i<num_queries;i++{
		elements:=extractIndexDefinitionField(query)
		select_block:=genSelectBlock(dataset)
		keyspace:=genKeySpace(query)
		where_block:=genWhereBlock(elements)
		var final_query string
		if array_fields==""{
			final_query = fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1",select_block,keyspace,where_block)
		}else{
			final_query = fmt.Sprintf("SELECT %s FROM %s WHERE ANY %s IN %s SATISFIES %s END",select_block,keyspace,where_block,where_block,where_block)
		}
		genrated_queries = append(genrated_queries, final_query)
	}
	for i,ele:=range genrated_queries{
		fmt.Printf("Query num %d is %s\n",i+1,ele)
	}
	return genrated_queries

}
func connect_cluster(queries []string, query_ip string, username string, password string) {
	
	// For a secure cluster connection, use `couchbases://<your-cluster-ip>` instead.
	cluster, err := gocb.Connect("couchbase://"+query_ip, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	err = cluster.WaitUntilReady(30*time.Second, nil)
	if err != nil {
		log.Fatalf("Failed to initialize cluster: %v", err)
	}
	log.Println("Connected to the Couchbase cluster")
	initial_time := time.Now()
	defer cluster.Close(nil)
	// errors:=0
	// success:=0
	total_number:=len(queries)
	log.Println("len of queries : ",total_number)
	wg.Add(total_number)
	for _,query:= range queries{
		go runQueries(cluster, query)
		// go func(q string){
		// 	defer wg.Done()
		// 	err := runQueries(cluster, query)
		// 	// mutex.Lock()
		// 	// defer mutex.Unlock()
		// 	if err != nil {
		// 		log.Println("Error executing query:", err)
		// 		// errors+=1
		// 		return
		// 	}
		// 	// success+=1
		
		// }(query)
	}
	wg.Wait()
	final_time := time.Since(initial_time)
	defer func ()  {
		log.Println(final_time)
		log.Printf("Sucuessful queries : %d\n",success)
		log.Printf("Failed queries : %d\n",errors)
		log.Printf("Pending queries : %d\n",total_number-(success+errors))
	}()

	// for _, query := range queries {
	// 	log.Println(query)
	// 	result, err := cluster.Query(query, &gocb.QueryOptions{Adhoc: true})
	// 	log.Println("Executing queries")
	// 	// fmt.Print(results)
	// 	if err != nil {
	// 		log.Printf("Query execution error: %v", err)
	// 		// http.Error(w, "Failed to execute query", http.StatusInternalServerError)
	// 		return
	// 	}
	// 	var r interface{}
	// 	for result.Next() {
	// 		err := result.Row(&r)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		fmt.Println("result *******-", r)
	// 	}
	// }
}
func runQueries(cluster *gocb.Cluster, queries string)error {
	defer wg.Done()
	rows, err := cluster.Query(queries, &gocb.QueryOptions{Adhoc: true})
	// err
	if err != nil {
		errors+=1
		log.Printf("Error executing query: %v", err)
		return err
	}
	success+=1
	defer rows.Close()
	return nil
}
func main(){
	var nodeAddress string
	var username string
	var password string
	var create_query string
	var dataset string
	var num_queries int
	
	flag.StringVar(&nodeAddress, "nodeAddress", "","ip address of the node")
	flag.StringVar(&username, "username", "", "username of the node")
	flag.StringVar(&password, "password", "", "password of the node")
	flag.StringVar(&create_query, "create_query", "", "create query for which select queries are genrated")
	flag.StringVar(&dataset, "dataset", "hotel", "Dataset for which the queries are generated")
	flag.IntVar(&num_queries, "num_queries", 10, "no of queries to be generated for particular create query")
	flag.Parse()

	//str := "CREATE INDEX `idx3_idxprefix` ON keyspacenameplaceholder(`name`,`phone`)"
	// create_definitions :=[]string{"CREATE INDEX `idx12_idxprefix` ON `b1.test_scope_1.test_collection_1`(`name` INCLUDE MISSING DESC,`phone`,`type`)","CREATE INDEX `idx12_idxprefix` ON keyspacenameplaceholder(`name` INCLUDE MISSING DESC,`phone`,`type`)","CREATE INDEX `idx13_idxprefix` ON keyspacenameplaceholder(`city` INCLUDE MISSING ASC, `phone`)"}

	query_list:=queryBuilder(create_query,num_queries,dataset)
	// fmt.Println(queries)
	// ip :="192.168.64.26"
	// username := "Administrator"
	// password := "password"
	connect_cluster(query_list, nodeAddress, username, password)
	// idx_def:=extractIndexDefinitionField(str)
	// select_block:=genSelectBlock()
	// where_block:=genWhereBlock(idx_def)

	// fmt.Printf("SELECT %s FROM keyspace where %s\n",select_block,where_block)
	
}