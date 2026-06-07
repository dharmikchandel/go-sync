package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "go-sync/proto"
)

const (
	serverAddr = "localhost:50051"
	chunkSize  = 64 * 1024 // 64KB
)

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}

	switch os.Args[1] {
	case "upload":
		if len(os.Args) != 3 {
			fmt.Println("usage: go-sync upload <file>")
			return
		}
		uploadFile(os.Args[2])

	case "watch":
		watchEvents()

	case "download":
		if len(os.Args) != 3 {
			fmt.Println("usage: go-sync download <filename>")
			return
		}
		downloadFile(os.Args[2])

	default:
		usage()
	}
}

func usage() {
	fmt.Println(`go-sync CLI Commands:
  		go-sync upload <file>
  		go-sync watch
		go-sync download <file>`)
}

func connect() pb.FileSyncServiceClient {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	return pb.NewFileSyncServiceClient(conn)
}

func uploadFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	client := connect()

	stream, err := client.UploadFile(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, chunkSize)
	filename := filepath.Base(path)

	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		err = stream.Send(&pb.UploadChunk{
			FileName: filename,
			Data:     buf[:n],
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Upload result:", resp.Message)
}

func watchEvents() {
	client := connect()

	stream, err := client.SyncEvents(context.Background(), &pb.SyncRequest{
		UserId: "user1",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Watching for file events...")

	for {
		event, err := stream.Recv()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf(
			"%s | %s v%d (%d bytes)\n",
			event.EventType,
			event.Filename,
			event.Version,
			event.SizeBytes,
		)
	}
}

func downloadFile(filename string) {
	client := connect()

	stream, err := client.DownloadFile(
		context.Background(),
		&pb.DownloadRequest{Filename: filename},
	)
	if err != nil {
		log.Fatal(err)
	}

	out, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		_, err = out.Write(chunk.Data)
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Downloaded:", filename)
}