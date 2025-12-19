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

// ----------------------------------------------------
// 報價數據結構 (已修正為匹配 Aster 實際報價 JSON 的所有頂層欄位)
// ----------------------------------------------------
type OrderbookMessage struct {
    // 交易所事件類型 (e.g., depthUpdate)
    Event string `json:"e"` 
    
    // 伺服器發送時間 (E)
    ServerSendTime int64 `json:"E"` 
    
    // 報價交易時間 (T) - 我們用這個來計算延遲
	EventTime int64 `json:"T"` 

    // 交易對 Symbol
    Symbol string `json:"s"`

    // Update ID 欄位
    U int64 `json:"U"`
    u int64 `json:"u"`
    pu int64 `json:"pu"`
    
    // 買單 (Bid) 數據 - 使用 json.RawMessage 類型安全地忽略複雜陣列
    Bids json.RawMessage `json:"b"` 
    
    // 賣單 (Ask) 數據 - 使用 json.RawMessage 類型安全地忽略複雜陣列
    Asks json.RawMessage `json:"a"` 
}

// ----------------------------------------------------
// 連線與訂閱設定
// ----------------------------------------------------
const (
    // 使用實際接收到的 Symbol (例如 ETHUSDT)
    ASTER_SYMBOL = "ethusdt"
    // 猜測深度流為 @depth (如果數據量太大，可嘗試 @depth5)
    ASTER_STREAM = "@depth" 
)

func main() {
    // ------------------ (1) 讀取 .env 檔案 ------------------
    if err := godotenv.Load(); err != nil {
        log.Println("警告: .env 檔案讀取失敗，將嘗試從系統環境變數獲取。")
    }

    // ------------------ (2) 構造 WebSocket URL ------------------
    base_url := os.Getenv("ASTER_WS_URL")
    if base_url == "" {
        log.Fatal("致命錯誤: 環境變數 ASTER_WS_URL 未設定。請檢查 .env 檔案。")
    }
    
    // URL = BaseURL + /ws/ + streamName (例如: wss://.../ws/ethusdt@depth)
    streamName := ASTER_SYMBOL + ASTER_STREAM
    url := fmt.Sprintf("%s/ws/%s", base_url, streamName)
    
    // ------------------ (3) 連接 WebSocket ------------------
    log.Printf("嘗試連接到 Asterisk 交易對: %s (完整URL: %s)", ASTER_SYMBOL, url)
    conn, _, err := websocket.DefaultDialer.Dial(url, nil)
    if err != nil {
        log.Fatalf("無法連接到 Asterisk WebSocket: %v", err)
    }
    defer conn.Close()
    
    fmt.Println("成功連線。等待數據推送...")


    // ------------------ (4) 延遲測量循環 (已修正所有變數宣告和邏輯) ------------------
    for {
        // 【修正：確保所有變數在循環內被宣告】
        
        // 讀取 WebSocket 訊息
        _, message, err := conn.ReadMessage()
        if err != nil {
            log.Printf("讀取錯誤 (連線可能被伺服器關閉): %v", err)
            return
        }
        
        // 獲取伺服器接收時間 (毫秒)
        serverReceiveTime := time.Now().UnixNano() / int64(time.Millisecond) 

        // 宣告數據結構
        var data OrderbookMessage
        
        // 嘗試解析 JSON
        if err := json.Unmarshal(message, &data); err != nil {
            // 由於可能收到 PING/PONG 或訂閱確認，無法解析是正常的。
            // log.Printf("JSON 解析失敗 (非報價數據?): %s", message)
            continue // 跳過這條無法解析的消息
        }

        // 提取交易所時間戳 (T)
        exchangeTimestamp := data.EventTime 
        
        // 進行延遲計算 (單位: 毫秒)
        latency := serverReceiveTime - exchangeTimestamp
        
        // 最終邏輯：只要時間戳有效，就打印結果 (允許負數延遲)
        if exchangeTimestamp > 0 { 
            log.Printf("🎉 接收到報價更新. 延遲: %d ms", latency)
        } else {
            // 如果解析出時間戳為 0，則打印原始數據以便偵錯
            log.Printf("⚠️ 時間戳無效. 原始數據: %s", message)
        }
    }
}