package main

import (
	"fmt"
	"log"
	"os"
	"time"
	"encoding/json"
	
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

// GRVT Mini Ticker 訂閱訊息 (最基本的報價流)
// 如果這個失敗，代表公共數據流連線有嚴重問題。
const subscribeMsg = `{
    "stream": "v1.ticker.s", 
    "feed": ["ETH_USD_Perp"], 
    "method": "subscribe",
    "is_full": true 
}`

// ----------------------------------------------------
// GRVT 數據結構 (處理巢狀結構和奈秒時間戳)
// ----------------------------------------------------

// FeedData 是實際的報價內容，包含 event_time (奈秒)
type FeedData struct {
	EventTime int64 `json:"event_time"` // 奈秒時間戳！
    // Mini Ticker 數據還包含其他欄位，但我們只需要這個時間戳
}

// GRVTStreamMessage 是頂層的 WebSocket 訊息結構
type GRVTStreamMessage struct {
    Stream string `json:"stream"`
    Selector string `json:"selector"`
    SequenceNumber string `json:"sequence_number"`
    Feed json.RawMessage `json:"feed"` // 接收實際的報價數據
}

func main() {
    if err := godotenv.Load(); err != nil {
        log.Println("警告: .env 檔案讀取失敗。")
    }

    // ------------------ (1) 構造 WebSocket URL ------------------
    // 使用正確的公共市場數據 URL
    url := os.Getenv("GRVT_WS_URL")
    if url == "" {
        log.Fatal("致命錯誤: 環境變數 GRVT_WS_URL 未設定。請檢查 .env 檔案。")
    }
    
    // ------------------ (2) 連接 WebSocket ------------------
    log.Printf("嘗試連接到 GRVT: %s", url)
    conn, _, err := websocket.DefaultDialer.Dial(url, nil)
    if err != nil {
        log.Fatalf("無法連接到 GRVT WebSocket: %v", err)
    }
    defer conn.Close()
    
    // ------------------ (3) 訂閱 Mini Ticker ------------------
    if err := conn.WriteMessage(websocket.TextMessage, []byte(subscribeMsg)); err != nil {
        log.Fatalf("訂閱失敗: %v", err)
    }
    
    fmt.Println("成功訂閱 GRVT Mini Ticker。等待數據推送...")

    // ------------------ (4) 延遲測量循環 ------------------
    for {
        _, message, err := conn.ReadMessage()
        if err != nil {
            log.Printf("讀取錯誤: %v", err)
            return
        }
        
        // 獲取伺服器接收時間 (奈秒)
        serverReceiveTimeNano := time.Now().UnixNano() 
        
        var streamMsg GRVTStreamMessage
        if err := json.Unmarshal(message, &streamMsg); err != nil {
            // 可能是訂閱成功的初始 JSONRPC 回應，或 PING/PONG 訊息，忽略
            continue 
        }

        // 檢查是否為有效的報價數據
        if streamMsg.Stream != "v1.ticker.s" || len(streamMsg.Feed) == 0 {
            // log.Printf("收到非 Ticker 訊息: %s", message)
            continue
        }

        // 第二次解析：解析 Feed 數據
        var feedData FeedData
        if err := json.Unmarshal(streamMsg.Feed, &feedData); err != nil {
            log.Printf("GRVT Feed 解析失敗: %v, 原始數據: %s", err, streamMsg.Feed)
            continue
        }
        
        // 奈秒轉換為毫秒
        exchangeTimestampMilli := feedData.EventTime / int64(time.Millisecond)
        serverReceiveTimeMilli := serverReceiveTimeNano / int64(time.Millisecond)

        // 進行延遲計算 (單位: 毫秒)
        latency := serverReceiveTimeMilli - exchangeTimestampMilli
        
        if exchangeTimestampMilli > 0 { 
            log.Printf("🎉 GRVT Ticker 更新. 延遲: %d ms", latency)
        } else {
            // 如果解析出時間戳為 0，則打印原始數據以便偵錯
            log.Printf("⚠️ GRVT 時間戳無效. 原始數據: %s", message)
        }
    }
}