package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
)

func main() {
	ctx := context.Background()
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
		Headers: map[string]string{
			"X-OpenAI-Api-Key":      "<the-key>",
			"X-Cohere-Api-Key":      "",
			"X-HuggingFace-Api-Key": "",
		},
	}
	client, err := weaviate.NewClient(cfg)
	if err != nil {
		panic(err)
	}

	// Specify a vectorizer.
	classObj := &models.Class{
		Class:      "Question",        // The class we are going to store.
		Vectorizer: "text2vec-openai", // Or "text2vec-cohere", "text2vec-huggingface"
	}

	if err := client.
		Schema().
		ClassCreator().
		WithClass(classObj).
		Do(ctx); err != nil {
		fmt.Println("create classObj error:", err)
	}
	GetSchema(client)

	f, err := download(
		"https://raw.githubusercontent.com/weaviate-tutorials/quickstart/main/data/jeopardy_tiny.json",
		"jeopardy_tiny.json",
	)
	if err != nil {
		log.Println("err:", err)
	} else {
		log.Println("downloaded")
	}
	defer f.Close()

	var items []map[string]string
	if err := json.NewDecoder(f).Decode(&items); err != nil {
		panic(err)
	}

	fmt.Println("items")
	for _, item := range items {
		fmt.Println(item)
	}

	// convert items into a slice of models.Object
	objects := make([]*models.Object, len(items))
	for i := range items {
		objects[i] = &models.Object{
			Class: "Question",
			Properties: map[string]any{
				"category": items[i]["Category"],
				"question": items[i]["Question"],
				"answer":   items[i]["Answer"],
			},
		}
	}

	// Batch write items.
	batchRes, err := client.Batch().ObjectsBatcher().WithObjects(objects...).Do(ctx)
	if err != nil {
		panic(err)
	}

	for _, res := range batchRes {
		if res.Result.Errors != nil {
			for _, err := range res.Result.Errors.Error {
				log.Fatalf("batch load failed: %#v\n", err)
			}
		}
	}
	// Verify: curl http://localhost:8080/v1/objects | jq
}

func GetSchema(client *weaviate.Client) {
	schema, err := client.Schema().Getter().Do(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v", schema)

}

func download(downloadURL, filePath string) (*os.File, error) {
	if fileExists(filePath) {
		return os.Open(filePath)
	}

	_, err := url.Parse(downloadURL)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Get(downloadURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	f, err := os.Create(filePath)
	if err != nil {
		f.Close()
		return nil, err
	}

	_, err = io.Copy(f, resp.Body)
	if err != nil {
		f.Close()
		return nil, err
	}

	return f, nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}

	return !info.IsDir()
}
