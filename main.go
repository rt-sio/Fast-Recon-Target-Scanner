package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Result struct {
	URL        string
	StatusCode int
	Err        error
}

func worker(jobs <-chan string, results chan<- Result, client *http.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	for url := range jobs {

		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			results <- Result{URL: url, Err: err}
			continue
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

		resp, err := client.Do(req)
		if err != nil {
			results <- Result{URL: url, Err: err}
			continue
		}
		resp.Body.Close()

		results <- Result{URL: url, StatusCode: resp.StatusCode}
	}

}

func main() {

	filename := "target.txt"
	numWorkers := 20

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	jobs := make(chan string, 100)
	results := make(chan Result, 100)

	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Критическая ошибка при открытии файла: %v\n", err)
		return
	}

	defer file.Close()

	var workerwg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		workerwg.Add(1)
		go worker(jobs, results, client, &workerwg)
	}

	var outputWg sync.WaitGroup
	outputWg.Add(1)

	go func() {
		defer outputWg.Done()
		for res := range results {
			if res.Err != nil {
				fmt.Printf("[ERR] %s -> %v\n", res.URL, res.Err)
				continue
			}
			switch {
			case res.StatusCode == http.StatusOK:
				fmt.Printf("[+] [200 OK]  Активный хост: %s\n", res.URL)
			case res.StatusCode >= 300 && res.StatusCode < 400:
				fmt.Printf("[*] [%d REDIRECT] Редирект: %s\n", res.StatusCode, res.URL)
			case res.StatusCode == http.StatusForbidden || res.StatusCode == http.StatusUnauthorized:
				fmt.Printf("[!] [%d AUTH] Требуется авторизация: %s\n", res.StatusCode, res.URL)
			default:
				fmt.Printf("[-] [%d] Зафиксирован статус: %s\n", res.StatusCode, res.URL)
			}
		}
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		domain := strings.TrimSpace(scanner.Text())
		if domain == "" || strings.HasPrefix(domain, "#") {
			continue
		}
		if !strings.HasPrefix(domain, "http://") && !strings.HasPrefix(domain, "https://") {
			domain = "https://" + domain
		}

		jobs <- domain
	}
	close(jobs)
	workerwg.Wait()
	close(results)

	outputWg.Add(1)

	fmt.Println("[*] Разведка успешно завершена.")
}
