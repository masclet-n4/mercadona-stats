package main

import (
	"fmt"
	"log"
	"mercadona-stats/internal/pocketbase"
	"mercadona-stats/internal/workers"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const numWorkers = 8

func filterFrom(records []pocketbase.PriceRecord, from time.Time) []pocketbase.PriceRecord {
	for i, r := range records {
		if !r.FechaMuestreo.Time().Before(from) {
			return records[i:]
		}
	}
	return nil
}

func parseSchedule(s string) (int, int, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("formato inválido %q, usar HH:MM", s)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h < 0 || h > 23 {
		return 0, 0, fmt.Errorf("hora inválida %q", parts[0])
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 0 || m > 59 {
		return 0, 0, fmt.Errorf("minuto inválido %q", parts[1])
	}
	return h, m, nil
}

func durationUntil(hour, min int) time.Duration {
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, min, 0, 0, now.Location())
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next.Sub(now)
}

func pbTimeFormat(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.000Z")
}

type producerResult struct {
	sent     int
	skipped  int
	fetchErr int
	errors   []string
}

func run(pbURL, adminEmail, adminPassword string) {
	startTime := time.Now()
	log.Println("--- Ejecución iniciada ---")

	pb := pocketbase.NewClient(pbURL)

	if err := pb.AuthenticateAdmin(adminEmail, adminPassword); err != nil {
		log.Printf("Error auth: %v", err)
		return
	}
	if err := pb.EnsureStatsCollection(); err != nil {
		log.Printf("Error ensure collection: %v", err)
		return
	}

	// Crear job con status "running"
	jobID, err := pb.CreateJob(map[string]any{
		"type":       "mercadona_stats",
		"status":     "running",
		"start_date": pbTimeFormat(startTime),
		"errors":     []string{},
		"details":    map[string]any{},
	})
	if err != nil {
		log.Printf("Error creando job: %v (se continúa sin tracking)", err)
	} else {
		log.Printf("Job creado: %s", jobID)
	}

	// Helper para actualizar el job al salir (si se pudo crear)
	finishJob := func(status string, details map[string]any, jobErrors []string) {
		if jobID == "" {
			return
		}
		if err := pb.UpdateJob(jobID, map[string]any{
			"status":   status,
			"end_date": pbTimeFormat(time.Now()),
			"details":  details,
			"errors":   jobErrors,
		}); err != nil {
			log.Printf("Error actualizando job: %v", err)
		}
	}

	products, err := pb.GetAllProducts()
	if err != nil {
		log.Printf("Error obteniendo productos: %v", err)
		finishJob("failed", map[string]any{}, []string{fmt.Sprintf("get products: %v", err)})
		return
	}
	log.Printf("Productos: %d", len(products))

	statsIDs, err := pb.GetStatsIdsMap()
	if err != nil {
		log.Printf("Error obteniendo stats: %v", err)
		finishJob("failed", map[string]any{"total_products": len(products)}, []string{fmt.Sprintf("get stats map: %v", err)})
		return
	}
	log.Printf("Stats existentes: %d", len(statsIDs))

	jobs := make(chan workers.Job, numWorkers)
	results := workers.Start(numWorkers, jobs)
	log.Printf("Pool arrancada con %d workers", numWorkers)

	// Productor — envía stats de vuelta por canal
	prodDone := make(chan producerResult, 1)
	go func() {
		defer close(jobs)
		now := time.Now()
		total := len(products)
		sent, skipped, fetchErr := 0, 0, 0
		var prodErrors []string
		for i, p := range products {
			if i > 0 && i%100 == 0 {
				log.Printf("[productor] %d/%d (%.0f%%) — enviados=%d sin-historial=%d errores=%d",
					i, total, float64(i)/float64(total)*100, sent, skipped, fetchErr)
			}
			history, err := pb.GetPriceHistory(p.ID, now.AddDate(0, 0, -365))
			if err != nil {
				log.Printf("[productor] error historial %s: %v", p.ID, err)
				prodErrors = append(prodErrors, fmt.Sprintf("fetch %s: %v", p.ID, err))
				fetchErr++
				continue
			}
			if len(history) == 0 {
				skipped++
				continue
			}
			jobs <- workers.Job{
				ProductID:   p.ID,
				History6d:   filterFrom(history, now.AddDate(0, 0, -6)),
				History30d:  filterFrom(history, now.AddDate(0, 0, -30)),
				History90d:  filterFrom(history, now.AddDate(0, 0, -90)),
				History180d: filterFrom(history, now.AddDate(0, 0, -180)),
				History365d: history,
			}
			sent++
		}
		log.Printf("[productor] terminado — enviados=%d sin-historial=%d errores=%d", sent, skipped, fetchErr)
		prodDone <- producerResult{sent, skipped, fetchErr, prodErrors}
	}()

	// Consumidor
	ok, fail := 0, 0
	var upsertErrors []string
	for stats := range results {
		stats.ID = statsIDs[stats.ProductID]
		if err := pb.UpsertStats(&stats); err != nil {
			log.Printf("[consumidor] error upsert %s: %v", stats.ProductID, err)
			upsertErrors = append(upsertErrors, fmt.Sprintf("upsert %s: %v", stats.ProductID, err))
			fail++
			continue
		}
		ok++
		if ok%100 == 0 {
			log.Printf("[consumidor] %d guardadas, %d errores", ok, fail)
		}
	}

	// Recoger métricas del productor
	prod := <-prodDone
	elapsed := time.Since(startTime)

	// Determinar status del job
	jobStatus := "completed"
	if ok == 0 && (fail > 0 || prod.fetchErr > 0) {
		jobStatus = "failed"
	} else if fail > 0 || prod.fetchErr > 0 {
		jobStatus = "completed_with_errors"
	}

	// Unir errores de productor y consumidor
	allErrors := make([]string, 0, len(prod.errors)+len(upsertErrors))
	allErrors = append(allErrors, prod.errors...)
	allErrors = append(allErrors, upsertErrors...)

	finishJob(jobStatus, map[string]any{
		"total_products":     len(products),
		"existing_stats":     len(statsIDs),
		"products_processed": prod.sent,
		"products_skipped":   prod.skipped,
		"fetch_errors":       prod.fetchErr,
		"upserted":           ok,
		"upsert_errors":      fail,
		"num_workers":        numWorkers,
		"duration_s":         int(elapsed.Seconds()),
	}, allErrors)

	log.Printf("Completado en %s — upserted=%d errores=%d",
		elapsed.Round(time.Second), ok, fail)
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("Aviso: no se encontró .env, usando variables de entorno del sistema")
	}

	pbURL := os.Getenv("PB_BASE_URL")
	if pbURL == "" {
		pbURL = "http://localhost:8090"
	}
	adminEmail := os.Getenv("PB_ADMIN_EMAIL")
	adminPassword := os.Getenv("PB_ADMIN_PASSWORD")
	if adminEmail == "" || adminPassword == "" {
		log.Fatal("faltan variables de entorno PB_ADMIN_EMAIL y/o PB_ADMIN_PASSWORD")
	}

	schedule := os.Getenv("SCHEDULE_TIME")

	// Sin SCHEDULE_TIME → ejecutar una vez y salir
	if schedule == "" {
		run(pbURL, adminEmail, adminPassword)
		return
	}

	// Con SCHEDULE_TIME → daemon que ejecuta a diario a esa hora
	hour, min, err := parseSchedule(schedule)
	if err != nil {
		log.Fatalf("SCHEDULE_TIME inválido: %v", err)
	}
	log.Printf("Scheduler activo — ejecución diaria a las %02d:%02d", hour, min)

	for {
		d := durationUntil(hour, min)
		log.Printf("Próxima ejecución en %s", d.Round(time.Second))
		time.Sleep(d)
		run(pbURL, adminEmail, adminPassword)
	}
}
