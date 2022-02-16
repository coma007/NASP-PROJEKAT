package main

import "time"

type TokenBucket struct {
	maxToken int          // maksimalan broj tokena koji staje u baket
	currentToken int      // preostali broj tokena
	rate int64            // vreme poslednjeg resetovanja  (sekunde)
	lastTimestamp int64   // vreme poslednjeg punjenja baketa  (sekunde)
}

func NewTokenBucket(rate int64, maximumTokens int) *TokenBucket {
	return &TokenBucket{
		maxToken: maximumTokens,
		currentToken: maximumTokens,
		rate: rate,
		lastTimestamp: time.Now().Unix(),
	}
}

func (tb *TokenBucket) CheckRequest() bool{
	if time.Now().Unix()-tb.lastTimestamp > tb.rate {
		tb.lastTimestamp = time.Now().Unix()
		tb.currentToken = tb.maxToken
		return true
	}

	if tb.currentToken > 0 {
		tb.currentToken--
		return true
	}
	
	return false
}
