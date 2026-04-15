package workers

import (
	"log"
	"math"
	"mercadona-stats/internal/pocketbase"
	"sync"
)

type Job struct {
	ProductID   string
	History6d   []pocketbase.PriceRecord
	History30d  []pocketbase.PriceRecord
	History90d  []pocketbase.PriceRecord
	History180d []pocketbase.PriceRecord
	History365d []pocketbase.PriceRecord
}

func round5(v float64) float64 {
	return math.Round(v*1e5) / 1e5
}

func calcMean(history []pocketbase.PriceRecord) float64 {
	if len(history) == 0 {
		return 0
	}
	var sum float64
	for _, r := range history {
		sum += r.UnitPrice
	}
	return sum / float64(len(history))
}

func coefVariation(history []pocketbase.PriceRecord) float64 {
	if len(history) == 0 {
		return 0
	}
	mean := calcMean(history)
	if mean == 0 {
		return 0
	}
	var sumSq float64
	for _, r := range history {
		d := r.UnitPrice - mean
		sumSq += d * d
	}
	return math.Sqrt(sumSq/float64(len(history))) / mean
}

// priceChangePct devuelve el porcentaje de cambio entre el precio más antiguo
// y el más reciente. Historial ordenado ASCENDENTE: [0]=más antiguo, [len-1]=más reciente.
func priceChangePct(history []pocketbase.PriceRecord) float64 {
	if len(history) < 2 {
		return 0
	}
	oldest, newest := history[0].UnitPrice, history[len(history)-1].UnitPrice
	if oldest == 0 {
		return 0
	}
	return ((newest - oldest) / oldest) * 100
}

func countPriceChanges(history []pocketbase.PriceRecord) int {
	changes := 0
	for i := 1; i < len(history); i++ {
		if history[i].UnitPrice != history[i-1].UnitPrice {
			changes++
		}
	}
	return changes
}

func Start(numWorkers int, jobs <-chan Job) <-chan pocketbase.Stats {
	results := make(chan pocketbase.Stats, numWorkers)
	var wg sync.WaitGroup

	for i := range numWorkers {
		wg.Add(1)
		id := i + 1
		go func() {
			defer wg.Done()
			log.Printf("[worker %d] arrancado", id)
			defer log.Printf("[worker %d] terminado", id)
			for job := range jobs {
				stats := pocketbase.Stats{
					ProductID: job.ProductID,
					Mean:      round5(calcMean(job.History365d)),
					PriceChangePercentage: pocketbase.PriceChanges{
						D6:   round5(priceChangePct(job.History6d)),
						D30:  round5(priceChangePct(job.History30d)),
						D90:  round5(priceChangePct(job.History90d)),
						D180: round5(priceChangePct(job.History180d)),
						D365: round5(priceChangePct(job.History365d)),
					},
					Variation: pocketbase.Variation{
						D6:   round5(coefVariation(job.History6d)),
						D30:  round5(coefVariation(job.History30d)),
						D90:  round5(coefVariation(job.History90d)),
						D180: round5(coefVariation(job.History180d)),
						D365: round5(coefVariation(job.History365d)),
					},
					CountPriceChanges: pocketbase.PriceChangeCounts{
						D6:   countPriceChanges(job.History6d),
						D30:  countPriceChanges(job.History30d),
						D90:  countPriceChanges(job.History90d),
						D180: countPriceChanges(job.History180d),
						D365: countPriceChanges(job.History365d),
					},
				}
				results <- stats
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}
