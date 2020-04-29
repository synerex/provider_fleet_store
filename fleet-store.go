package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	fleet "github.com/synerex/proto_fleet"

	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// datastore provider provides Datastore Service.

type DataStore interface {
	store(str string)
}

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              sync.Mutex
	version         = "0.01"
	baseDir         = "store"
	dataDir         string
	sxServerAddress string
	ds              DataStore
)

func init() {
	var err error
	dataDir, err = os.Getwd()
	if err != nil {
		fmt.Printf("Can't obtain current wd")
	}
	dataDir = filepath.ToSlash(dataDir) + "/" + baseDir
	ds = &FileSystemDataStore{
		storeDir: dataDir,
	}
}

type FileSystemDataStore struct {
	storeDir  string
	storeFile *os.File
	todayStr  string
}

// open file with today info
func (fs *FileSystemDataStore) store(str string) {
	const layout = "2006-01-02"
	day := time.Now()
	todayStr := day.Format(layout) + ".csv"
	if fs.todayStr != "" && fs.todayStr != todayStr {
		fs.storeFile.Close()
		fs.storeFile = nil
	}
	if fs.storeFile == nil {
		_, er := os.Stat(fs.storeDir)
		if er != nil { // create dir
			er = os.MkdirAll(fs.storeDir, 0777)
			if er != nil {
				fmt.Printf("Can't make dir '%s'.", fs.storeDir)
				return
			}
		}
		fs.todayStr = todayStr
		file, err := os.OpenFile(filepath.FromSlash(fs.storeDir+"/"+todayStr), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("Can't open file '%s'", todayStr)
			return
		}
		fs.storeFile = file
	}
	fs.storeFile.WriteString(str + "\n")
}

func supplyFleetCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

	flt := &fleet.Fleet{}

	err := proto.Unmarshal(sp.Cdata.Entity, flt)
	if err == nil { // get Fleet
		ts0 := ptypes.TimestampString(sp.Ts)
		ld := fmt.Sprintf("%s,%d,%f,%f,%f,%d", ts0, flt.VehicleId, flt.Coord.Lat, flt.Coord.Lon, flt.Angle, flt.Speed)
		log.Printf(ld)
		ds.store(ld)
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.Client != nil {
		client.Client = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.Client == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server\n", sxServerAddress)
	}
	mu.Unlock()
}

func subscribeFleetSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyFleetCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func main() {
	log.Printf("Fleet-Store(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.RIDE_SHARE}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "FleetStore", channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	pc_client := sxutil.NewSXServiceClient(client, pbase.RIDE_SHARE, "{Client:FleetStore}")

	wg.Add(1)
	log.Print("Subscribe Supply")
	go subscribeFleetSupply(pc_client)

	wg.Wait()

}
