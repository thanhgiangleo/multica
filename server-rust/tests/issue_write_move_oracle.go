package main

import (
    "encoding/json"
    "fmt"
    "math"
)

func main() {
    for _, value := range []float64{2, math.NaN(), math.Inf(1)} {
        encoded, err := json.Marshal(value)
        fmt.Printf("value=%v encoded=%s error=%v\n", value, encoded, err != nil)
    }
}
