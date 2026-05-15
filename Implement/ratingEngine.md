# Rating Engine - Thiết kế Chi tiết (Tầng Gold)

## 1. Tổng quan

Hệ thống chấm điểm cầu thủ tự động dựa trên hiệu suất thi đấu thực tế từ Sofascore, phân bổ trọng số theo từng vị trí cụ thể (Sub-Position). Mục tiêu: tìm ra cầu thủ thực sự xuất sắc và **"Underdog"** (gánh team ở đội yếu).

> **Nguyên tắc cốt lõi**: Rating chỉ đánh giá **Performance**, không dùng Market Value. Market Value là output riêng để phân tích sau.

---

## 2. Pipeline 4 Bước

```
Raw Data → B1: Threshold Filter → B2: P90 Normalize → B3: Min-Max Scale (theo League) → B4: Weighted Score + Penalty + Team Bonus
```

### Bước 1: Threshold Filter
Loại bỏ mẫu nhỏ (Small Sample):

```
IF Minutes_Played < 900 THEN Status = "Small Sample" (SKIP)
```

### Bước 2: Per 90 Standardization
Đưa mọi chỉ số đếm về cùng hệ quy chiếu 1 trận đấu:

```
Metric_p90 = (Metric_raw / Minutes_Played) × 90
```

### Bước 3: Min-Max Scaling (theo League)
Chuẩn hóa thang điểm 0-100 **theo từng giải đấu** (không gộp global):

```
Scaled = (x - x_min_league) / (x_max_league - x_min_league) × 100
```

> Lý do: mỗi league chỉ có top 10 cầu thủ. Scale theo league đảm bảo "Cầu thủ xuất sắc nhất giải" luôn đạt 100 điểm, công bằng giữa các giải.

### Bước 4: Tính Final Score

```
Base_Score = Σ(Scaled_i × Weight_i)            // Cộng điểm có trọng số
Penalty    = (Scaled_PenaltyMetric / 100) × PenaltyWeight   // Trừ điểm tuyệt đối
Team_Bonus = 1.0 + 0.015 × (Team_Rank - 1)    // Hệ số gánh team (Underdog Bonus)

Final_Score = (Base_Score - Penalty) × Team_Bonus     // Scale 0-100
```

> **Underdog Bonus**: Đội yếu (rank cao) được thưởng điểm, đội mạnh (rank thấp) giữ nguyên. VD: rank 20 → ×1.285, rank 1 → ×1.0

---

## 3. Phân bổ Trọng số (Weights) theo Vị trí

### Nhóm Tấn công & Tạo đột biến

#### ST / CF (Centre-Forward) — "Sát thủ vòng cấm"

| Loại | Chỉ số | Trọng số |
|------|--------|---------|
| Cộng | Base Rating | 30% |
| Cộng | Goals P90 | 25% |
| Cộng | xG P90 | 15% |
| Cộng | Shots on Target P90 | 10% |
| Cộng | Goal Conversion % | 10% |
| Cộng | Big Chances Created P90 | 10% |
| Trừ | Possession Lost | -5% |
| Trừ | Big Chances Missed | -5% |

#### RW / LW / RM / LM — "Đột phá & Kiến tạo"

| Loại | Chỉ số | Trọng số |
|------|--------|---------|
| Cộng | Base Rating | 30% |
| Cộng | Successful Dribbles P90 | 20% |
| Cộng | Assists P90 | 15% |
| Cộng | xA P90 | 15% |
| Cộng | Goals P90 | 10% |
| Cộng | Big Chances Created P90 | 10% |
| Trừ | Possession Lost | -5% |
| Trừ | Big Chances Missed | -5% |

#### AM (Attacking Midfield) — "Bộ não sáng tạo"

| Loại | Chỉ số | Trọng số |
|------|--------|---------|
| Cộng | Base Rating | 30% |
| Cộng | Key Passes P90 | 20% |
| Cộng | xA P90 | 15% |
| Cộng | Assists P90 | 15% |
| Cộng | Big Chances Created P90 | 10% |
| Cộng | Accurate Passes % | 10% |
| Trừ | Possession Lost | -5% |

---

### Nhóm Kiểm soát & Phòng ngự

#### CM (Central Midfield) — "Trạm trung chuyển"

| Loại | Chỉ số | Trọng số |
|------|--------|---------|
| Cộng | Base Rating | 30% |
| Cộng | Accurate Passes % | 20% |
| Cộng | Key Passes P90 | 15% |
| Cộng | xA P90 | 15% |
| Cộng | Assists P90 | 10% |
| Cộng | Big Chances Created P90 | 10% |
| Trừ | Possession Lost | -5% |

#### DM (Defensive Midfield) — "Máy quét"

| Loại | Chỉ số | Trọng số |
|------|--------|---------|
| Cộng | Base Rating | 30% |
| Cộng | Tackles P90 | 20% |
| Cộng | Interceptions P90 | 15% |
| Cộng | Ground Duels Won % | 15% |
| Cộng | Accurate Passes % | 10% |
| Cộng | xA / Key Pass P90 | 10% |
| Trừ | Possession Lost | -5% |

---

#### CB (Centre-Back) — "Hòn đá tảng"

| Loại | Chỉ số | Trọng số |
|------|--------|---------|
| Cộng | Base Rating | 30% |
| Cộng | Interceptions P90 | 20% |
| Cộng | Tackles P90 | 20% |
| Cộng | Aerial Duels Won % | 15% |
| Cộng | Ground Duels Won % | 15% |
| Trừ | **Errors Leading to Goal / Dribbled Past** | **-5%** |

> **Lưu ý**: CB không bị trừ Possession Lost (vì phất bóng dài/phá bóng là nhiệm vụ). Thay bằng Errors Lead Goal hoặc Dribbled Past - mới là "tội đồ" thực sự của trung vệ.

#### RB / LB (Full-Back) — "Công thủ toàn diện"

| Loại | Chỉ số | Trọng số |
|------|--------|---------|
| Cộng | Base Rating | 30% |
| Cộng | Tackles P90 | 20% |
| Cộng | Ground Duels Won % | 20% |
| Cộng | Interceptions P90 | 15% |
| Cộng | Assists / xA P90 | 15% |
| Trừ | Possession Lost | -5% |

---

#### GK (Goalkeeper)

| Loại | Chỉ số | Trọng số |
|------|--------|---------|
| Cộng | Base Rating | 30% |
| Cộng | Saves P90 | 30% |
| Cộng | Clean Sheets % | 20% |
| Cộng | Aerial Duels Won % | 20% |
| Trừ | ~~Goals Conceded P90~~ | **0% (ĐÃ BỎ)** |

> **Lý do bỏ Goals Conceded**: Gây double penalty cho GK đội yếu (vừa team_rank thấp, vừa thủng lưới nhiều). Thay vào đó, Save Percentage hoặc Goals Prevented (xG Faced - Goals Conceded) là chỉ số công bằng hơn - phản ánh đúng tài năng GK bất chấp đội bóng.

---

## 4. Công thức Tổng hợp

### B1 - Threshold

```
IF minutes < 900 THEN status = "Small Sample" (skip rating)
```

### B2 - P90

```
Metric_p90 = (Metric_raw / Minutes_Played) × 90
```

### B3 - Min-Max (theo League)

```
Scaled = (x - x_min_league) / (x_max_league - x_min_league) × 100
```

### B4 - Final Score

```
Base_Score   = Σ(Scaled_i × Weight_i)             // tổng có trọng số
Penalty      = (Scaled_PenaltyMetric / 100) × PenaltyWeight  // trừ tuyệt đối
Team_Bonus   = 1.0 + 0.015 × (Team_Rank - 1)     // Underdog Bonus

Final_Score  = (Base_Score - Penalty) × Team_Bonus   // 0-100
```

---

## 5. Output (Gold Layer)

### Bảng `gold_player_rating`

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `internal_player_id` | VARCHAR | PLR_xxxxx |
| `name` | VARCHAR | Tên cầu thủ |
| `sub_position` | VARCHAR | Centre-Forward, Left-Winger... |
| `league` | VARCHAR | Premier League, La Liga... |
| `team_name` | VARCHAR | Tên đội bóng |
| `team_rank` | INT | 1-20 |
| `minutes_played` | INT | Tổng số phút |
| `base_score` | FLOAT | 0-100 (trước multiplier & penalty) |
| `penalty` | FLOAT | Điểm trừ (0 đến -10) |
| `team_multiplier` | FLOAT | 1.0 - 1.285 |
| **`final_scout_score`** | **FLOAT** | **0-100 (kết quả cuối cùng)** |
| `status` | VARCHAR | Active / Small Sample |

---

## 6. Cấu trúc thư mục Implementation

```
Phase_3_Gold/
├── config/
│   └── position_weights.py      # 8 position configs (weights + penalty)
├── rating_engine.py             # Pipeline 4 bước
├── normalizer.py                # P90 + Min-Max helpers
└── output/
    └── data/                    # Kết quả parquet
```

---

## 7. Lưu ý Kỹ thuật

| Vấn đề | Giải pháp |
|--------|----------|
| Scale theo League hay Global? | **Scale theo League** (tránh top 1 league lấn át) |
| Cầu thủ ở đội mạnh được lợi? | **Underdog Bonus**: thưởng đội yếu, đội mạnh giữ nguyên |
| CB bị oan vì Possession Lost? | Bỏ PossLost cho CB, thay = Errors Lead Goal / Dribbled Past |
| GK bị double penalty? | Bỏ Goals Conceded, dùng Save Percentage / Goals Prevented |
| Small Sample? | Threshold 900 phút |
| Các metric khác đơn vị? | P90: % giữ nguyên, đếm được → P90 |
