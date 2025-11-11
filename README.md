# CropCast: Agricultural Price Forecasting System

> **A Learning-Focused Data Science Project**  
> Understanding how historical data, weather patterns, and machine learning come together to predict crop prices 7 days in advance.

**Project Guidance**: This project is developed under the guidance of **Professor K. Chelvakumar**, Visiting Professor of Mechanical Engineering at IIT Gandhinagar.

---

## What is CropCast?

CropCast is an educational data science project that demonstrates how we can use historical information to make informed predictions about the future. Specifically, it forecasts the prices of agricultural commodities (crops like potato, tomato, and onion) in Indian markets one week ahead.

Think of it like a weather forecast, but for crop prices. Just as meteorologists use past weather patterns, atmospheric pressure, and temperature to predict tomorrow's weather, CropCast uses historical price trends, weather data, and seasonal patterns to predict next week's crop prices.

### Why Does This Matter?

For farmers, knowing future prices helps them decide:
- **When to sell**: Should I sell today or wait for better prices next week?
- **What to grow**: Which crop is likely to be more profitable next season?
- **Where to sell**: Should I take my produce to Market A or Market B?

For policymakers and economists, price forecasts help:
- Detect abnormal price movements that might hurt consumers or farmers
- Plan interventions like releasing buffer stocks
- Understand market dynamics and seasonal patterns

---

## Project Goals

This project is designed as an **educational proof-of-concept** for a high school (Class 12) internship. The goals are:

1. **Learn by doing**: Experience the complete data science workflow from data collection to prediction
2. **Understand the process**: See how raw data becomes insights through systematic transformation
3. **Build confidence**: Start with simple concepts and gradually add complexity
4. **Create value**: Produce actual forecasts that could inform real decisions
5. **Practice best practices**: Write reproducible code, document decisions, validate results

**Important**: This is a learning project, not a production system. The predictions are for educational purposes and should not be used for actual trading or financial decisions.

---

## The Big Picture: How CropCast Works

### The Journey from Data to Predictions

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   DATA SOURCES  │ ───► │   DATA STORAGE  │ ───► │  DATA ANALYSIS  │ ───► │   PREDICTIONS   │
│                 │      │                 │      │                 │      │                 │
│ • Market prices │      │ • Bronze Layer  │      │ • Cleaning      │      │ • XGBoost Model │
│ • Weather data  │      │ • Silver Layer  │      │ • Merging       │      │ • 7-day forecast│
│ • Market info   │      │ • Gold Layer    │      │ • Features      │      │ • Confidence    │
└─────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘
```

Let's explore each step in detail.

---

## Step 1: Data Sources — Where Does Information Come From?

### Understanding Data Sources

Every prediction system starts with data. CropCast collects information from three trusted public sources:

#### 1. **AgMarketNet**
**What it is**: Government of India's agricultural marketing information portal  
**What we get**: Daily crop prices from mandis (wholesale markets) across India  
**Why it matters**: This is the "ground truth" — actual prices that farmers received

**Example data point**:
```
Date: 2024-08-15
Market: Delhi (Azadpur)
Commodity: Potato (Local variety)
Price: ₹2,250 per quintal (100 kg)
Arrival: 150 quintals brought to market that day
```

This tells us that on August 15th, farmers in Delhi sold potato at ₹2,250 per quintal.

#### 2. **eNAM (National Agriculture Market)**
**What it is**: Electronic trading platform connecting agricultural markets  
**What we get**: Real-time and historical prices from digitally connected mandis  
**Why it matters**: Modern, verified price data with quality checks

Think of eNAM as the "digital version" of traditional markets — same crops, same prices, but recorded electronically for accuracy.

#### 3. **Open-Meteo**
**What it is**: Free weather data API with historical and forecast information  
**What we get**: Daily temperature, rainfall, humidity for each market location  
**Why it matters**: Weather strongly influences crop supply and prices

**Connection to prices**: 
- Heavy rainfall → Damage to stored crops → Reduced supply → Higher prices
- Extreme heat → Lower harvest → Scarcity → Higher prices
- Good weather → Abundant harvest → More supply → Lower prices

### How We Collect Data: Web Scraping and APIs

**Web Scraping** is like copying information from websites automatically:
- We write Python scripts that visit AgMarketNet and eNAM websites
- The scripts read price tables just like a human would
- They save the data in structured files for analysis
- We respect robots.txt rules and add delays to be polite to servers

**APIs (Application Programming Interfaces)** are official channels for getting data:
- Open-Meteo provides a structured way to request weather data
- We send a request: "Give me temperature and rainfall for Delhi on August 15, 2024"
- The API responds with exact numbers in a standard format
- This is more reliable than scraping because it's designed for automation

---

## Step 2: Data Storage — The Medallion Architecture

### Why Do We Need a Storage Strategy?

Imagine you're researching a history project. You might:
1. Collect **raw materials**: newspaper clippings, photos, interviews (might contain errors)
2. Create **organized notes**: verify facts, group by theme, highlight key points
3. Write **final analysis**: essays, conclusions, insights based on verified information

CropCast follows the same pattern, called the **Medallion Architecture** (Bronze → Silver → Gold). This is a best practice in modern data engineering.

### Bronze Layer: Raw Materials

**Purpose**: Store original data *exactly as collected*, without changes

**Technology**: DuckDB (a fast, lightweight database that runs on your laptop)

**Structure**:
```
agmarknet_bronze_files
├── file_id (unique identifier)
├── checksum (data fingerprint to prevent duplicates)
├── original_filename
├── raw_payload (the actual JSON data collected)
├── country, state, district, market (location info)
├── year, month (when collected)
├── row_count (how many price records)
└── ingest_ts (timestamp of collection)
```

**Why keep raw data?**
- **Reproducibility**: We can always go back to original data if we make mistakes
- **Deduplication**: Using checksums, we avoid storing the same file twice
- **Audit trail**: We know exactly when and where each piece of data came from
- **Debugging**: If something looks wrong later, we can check the source

**Real-world analogy**: This is like keeping your original receipts even after entering them into a spreadsheet.

### Silver Layer: Cleaned and Organized

**Purpose**: Transform raw data into clean, analysis-ready format

**What happens here**:
1. **Cleaning**:
   - Remove duplicates
   - Fill or remove missing values
   - Fix inconsistent spellings (e.g., "Delhi" vs "DELHI" vs "delhi")
   - Validate data types (prices should be numbers, not text)

2. **Standardization**:
   - Convert all dates to YYYY-MM-DD format
   - Normalize location names ("New Delhi" → "delhi")
   - Use consistent units (all prices in ₹ per quintal)

3. **Merging**:
   - Join price data with weather data for the same date and location
   - Align market information with geographic coordinates

**Example transformation**:
```
Bronze (raw):
  "date": "15-08-2024", "market": "DELHI", "price": "2250.00"

Silver (cleaned):
  date: 2024-08-15, market: "delhi", modal_price: 2250.0, 
  temp_max: 35.2, rainfall: 5.3
```

**Why this matters**: Clean data is essential for accurate predictions. "Garbage in, garbage out" — if we feed messy data to our model, we'll get unreliable predictions.

### Gold Layer: Ready-to-Use Features

**Purpose**: Create specialized datasets optimized for machine learning

**What happens here**:
1. **Feature Engineering**: Create new variables that help predict prices:
   - **Lag features**: Yesterday's price, last week's price
   - **Moving averages**: Average price over the last 7 days
   - **Time features**: Day of week, month, agricultural season
   - **Weather aggregates**: 7-day rainfall total, average temperature

2. **Final validation**: Ensure no missing values, correct data types, logical values

**Example gold record**:
```
date: 2024-08-15
commodity: potato
market: delhi
price: 2250.0              ← This is what we want to predict
price_lag_1: 2240.0        ← Yesterday's price
price_lag_7: 2180.0        ← Price 7 days ago
price_ma_7: 2210.5         ← 7-day average
temp_max: 35.2
temp_min: 28.1
rainfall: 5.3
season: kharif             ← Monsoon crop season
day_of_week: 3             ← Thursday
month: 8                   ← August
```

**Why features matter**: Machine learning models learn patterns from features. Good features = better predictions.

---

## Step 3: Data Analysis — From Numbers to Insights

### Understanding the Data: Exploratory Data Analysis (EDA)

Before building prediction models, we need to **understand our data**. Think of this as getting to know a person before becoming friends.

**Questions we ask**:
1. **Completeness**: Do we have data for all days? Any missing gaps?
2. **Quality**: Are there obvious errors? (e.g., negative prices, impossible temperatures)
3. **Patterns**: Do prices follow seasonal trends? Weekly patterns?
4. **Relationships**: How strongly does rainfall affect prices? Do prices spike after heat waves?
5. **Outliers**: Are there unusual price jumps? Why did wheat spike 50% on that day?

**Tools we use**:
- **Jupyter Notebooks**: Interactive environment for exploration
- **Pandas**: Python library for data manipulation (like Excel, but more powerful)
- **Visualization**: Charts and graphs to see patterns visually

**Example insights**:
- "Potato prices are 15% higher during April-May (harvest season) compared to other months"
- "Heavy rainfall (>50mm) is followed by 5-8% price increase within 3 days"
- "Friday prices are typically 2-3% higher than Monday prices (weekend demand)"

### Building the Prediction Model: Machine Learning

**What is Machine Learning?**

Machine learning is teaching a computer to learn patterns from examples, just like how you learned to recognize animals:
- You saw many pictures of cats → Your brain learned what makes something a cat
- The model sees many examples of {weather, past prices} → {future price}
- After enough examples, it can predict prices for new situations

**Our Model: XGBoost**

**XGBoost** (Extreme Gradient Boosting) is a powerful prediction algorithm. Here's a simple analogy:

Imagine you're learning to predict exam scores. You might think:
- First guess: "Average score is 75, so I'll predict 75 for everyone"
- Then you notice: "Students who study >5 hours score +10 points more"
- Then: "Students in Math class score +5 points vs English"
- And so on...

XGBoost works similarly but with hundreds of such "rules" combined intelligently. It:
1. Starts with a simple average prediction
2. Looks at where it made mistakes
3. Builds a "decision tree" to correct those mistakes
4. Repeats this process many times (typically 100-1000 iterations)
5. Combines all trees for a final prediction

**Why XGBoost?**
- Handles complex, non-linear relationships (prices don't follow simple formulas)
- Works well with missing data
- Fast to train (minutes, not hours)
- Widely used in industry (won many Kaggle competitions)
- Interpretable (we can see which features are most important)

### Training Process: Teaching the Model

**Step 1: Split Data**
```
Total data: 1000 days of prices
├── Training set (700 days, 70%): Used to teach the model
├── Validation set (150 days, 15%): Used to tune hyperparameters
└── Test set (150 days, 15%): Final evaluation (model has never seen this)
```

**Why split?** To prevent "memorization" (overfitting). We want a model that generalizes to new data, not one that memorizes training examples.

**Step 2: Feature Selection**

We decide which features to include:
- Include: price_lag_1 to price_lag_7 (recent price history)
- Include: temp_max, temp_min, rainfall (weather)
- Include: day_of_week, month, season (time patterns)
- Exclude: file_id, ingest_ts (metadata not useful for prediction)

**Step 3: Hyperparameter Tuning**

Hyperparameters are "settings" for the model, like:
- `n_estimators`: How many decision trees to build (e.g., 100)
- `learning_rate`: How fast the model learns (e.g., 0.1)
- `max_depth`: How complex each tree can be (e.g., 5 levels)

We try different combinations and pick the one with best validation performance.

**Step 4: Training**

The model processes training data:
```
For each row:
  Input: [price_lag_1=2240, temp_max=35.2, rainfall=5.3, ...]
  Target: price = 2250
  
Model learns: "When recent price is 2240, temp is 35, rain is 5...
              then tomorrow's price is likely 2250"
```

After seeing 700 examples, the model has learned general patterns.

**Step 5: Evaluation**

We test on the **test set** (data the model has never seen):

**Metrics we track**:
1. **RMSE (Root Mean Squared Error)**:
   - Measures average prediction error in rupees
   - Target: RMSE < 20% of mean price
   - Example: If average price is ₹2,000, RMSE should be < ₹400

2. **MAE (Mean Absolute Error)**:
   - Average difference between predicted and actual price
   - More intuitive than RMSE
   - Example: MAE of ₹150 means average error is ₹150 per quintal

3. **R² Score (R-squared)**:
   - Measures how well model captures variance (0 to 1 scale)
   - Target: R² > 0.5 (explains at least 50% of price variation)
   - Example: R² = 0.7 means model explains 70% of price changes

**Interpreting Results**:
```
Model Performance:
RMSE: ₹325 (16% of mean price ₹2,000) Good
MAE: ₹180 Good
R²: 0.68 Good

Interpretation: 
- On average, predictions are off by ₹180
- Model explains 68% of price variations
- Acceptable for educational POC
```

### Feature Importance: What Matters Most?

After training, we can ask: "Which features are most important for predictions?"

**Example results**:
```
Feature Importance (%)
├── price_lag_1: 35%        ← Yesterday's price is strongest predictor
├── price_lag_7: 18%        ← Last week's price also important
├── price_ma_7: 15%         ← 7-day average captures trends
├── rainfall: 12%           ← Weather matters
├── temp_max: 8%
├── month: 6%               ← Seasonal patterns
└── day_of_week: 6%
```

**Insight**: Recent price history (lag features) are most predictive, followed by weather. This makes sense — prices tend to change gradually, not jump randomly.

---

## Key Concepts Explained

### 1. Time Series vs. Cross-Sectional Data

**Time Series Data**: Observations collected over time for the same subject
- Example: Daily potato prices in Delhi market (one market, many days)
- Key characteristic: Order matters! Yesterday's price helps predict today's price
- Challenge: Cannot shuffle data randomly — must respect time order

**Cross-Sectional Data**: Observations at a single point in time across different subjects
- Example: Potato prices in 100 different markets on one day
- Key characteristic: Order doesn't matter, observations are independent

**CropCast uses time series data**, which requires special handling:
- Train/test split must be chronological (can't use future to predict past!)
- Features include lagged values (looking backward in time)
- Seasonality and trends matter

### 2. Feature Engineering: Creating Predictive Variables

**What is a feature?** A measurable property used for prediction. Raw data often needs transformation to become useful features.

**Types of features in CropCast**:

1. **Lag Features**: Past values
   - `price_lag_1` = yesterday's price
   - `price_lag_7` = price 7 days ago
   - **Why useful**: Prices have momentum; yesterday's price is highly predictive

2. **Rolling Statistics**: Aggregations over time windows
   - `price_ma_7` = average price over last 7 days
   - **Why useful**: Smooths out daily noise, shows trends

3. **Temporal Features**: Extracted from dates
   - `day_of_week` = 0 (Monday) to 6 (Sunday)
   - `month` = 1 to 12
   - `season` = kharif/rabi/zaid (Indian agricultural seasons)
   - **Why useful**: Captures weekly/seasonal patterns

4. **External Features**: Information from outside sources
   - `temp_max`, `rainfall` from weather data
   - **Why useful**: Supply-side shocks affect prices

**Good features = Better predictions**. Much of a data scientist's work is creating meaningful features.

### 3. Overfitting vs. Underfitting

**Underfitting**: Model is too simple, misses important patterns
- Example: Predicting "tomorrow's price = average price" (ignores all trends)
- Symptom: Poor performance on both training and test data
- Solution: Use more complex model or add more features

**Overfitting**: Model is too complex, memorizes training data instead of learning patterns
- Example: Model learns "on Aug 15, 2023, price was ₹2,250" but can't generalize to 2024
- Symptom: Excellent training performance, poor test performance
- Solution: Simplify model, use regularization, get more data

**The sweet spot**: Model that captures true patterns but ignores noise. This is why we split data and evaluate on unseen test set.

### 4. The Medallion Architecture (Bronze-Silver-Gold)

**Why three layers?**

1. **Separation of concerns**: Each layer has a specific purpose
   - Bronze: Preserve raw truth
   - Silver: Clean and standardize
   - Gold: Optimize for specific use cases

2. **Reusability**: Silver layer can feed multiple gold layers
   - Gold layer for price prediction
   - Gold layer for market analysis
   - Gold layer for anomaly detection

3. **Debugging**: If gold layer has issues, check silver. If silver has issues, check bronze.

4. **Performance**: Gold layer is optimized for fast model training (no need to clean data every time)

**Industry standard**: Used by major companies (Databricks, Snowflake, AWS) for data platforms.

### 5. Model Versioning and Reproducibility

**The Problem**: "I trained a model last week with 75% accuracy. Today I retrained and got 60%. What changed?"

**The Solution**: Track everything
- **Code version**: Git commit hash
- **Data version**: Checksum of training data
- **Hyperparameters**: All model settings
- **Dependencies**: Python package versions
- **Training metrics**: RMSE, MAE, R² on training, validation, test
- **Training date and duration**: When and how long

**Example model metadata**:
```json
{
  "model_version": "1.0.0",
  "model_type": "xgboost",
  "commodity": "potato",
  "market": "delhi",
  "train_date": "2024-11-15",
  "data_start": "2023-01-01",
  "data_end": "2024-10-31",
  "train_size": 700,
  "test_size": 150,
  "hyperparameters": {
    "n_estimators": 100,
    "learning_rate": 0.1,
    "max_depth": 5
  },
  "test_metrics": {
    "rmse": 325.4,
    "mae": 180.2,
    "r2": 0.68
  },
  "training_time_seconds": 45.3
}
```

**Why it matters**: Scientific rigor. Results should be reproducible by anyone with the same data and code.

---

## Technology Stack

### Why These Tools?

#### DuckDB: Modern Analytical Database
**What it is**: An in-process SQL database optimized for analytics  
**Why we chose it**:
- Runs on your laptop (no server needed)
- Fast queries on large datasets (millions of rows in seconds)
- SQL interface (standard, widely-taught query language)
- Excellent for data science workflows
- Free and open-source

**Alternative considered**: SQLite (slower for analytics), PostgreSQL (requires server setup)

#### Python: The Data Science Language
**Why Python?**
- Standard language for data science and ML
- Rich ecosystem (pandas, scikit-learn, xgboost)
- Easy to learn for beginners
- Jupyter notebooks for interactive exploration
- Huge community and resources

**Alternative considered**: R (good for statistics but less general-purpose)

#### XGBoost: State-of-the-Art ML Algorithm
**Why XGBoost?**
- Best-in-class for tabular data (most Kaggle winners use it)
- Fast training and prediction
- Handles missing data gracefully
- Feature importance for interpretability
- Robust to outliers

**Alternative considered**: 
- Linear Regression (too simple, can't capture non-linear patterns)
- Neural Networks (overkill for this data size, harder to interpret)
- ARIMA (time-series specific but can't easily incorporate weather features)

#### Parquet: Efficient Data Storage Format
**Why Parquet?**
- Columnar format (fast for analytics)
- Built-in compression (saves 70-90% disk space)
- Type safety (prevents errors)
- Fast loading into pandas/DuckDB

**Alternative considered**: CSV (no compression, no type safety), JSON (verbose, slow)

---

## Project Structure

```
crop-ai/
│
├── data/                          # Raw data (gitignored)
│   ├── agmarknet/
│   │   └── 2023/
│   │       └── Aug/
│   │           └── 2023-08-15.jsonl.gz
│   ├── enam/
│   └── meteo/
│
├── pipeline/                      # ETL scripts
│   ├── crawl_agmarket.py          # Scrape AgMarketNet
│   ├── crawl_enam.py              # Scrape eNAM
│   ├── crawl_openmeteo.py         # Fetch weather data
│   ├── ingest_bronze_agmarket.py  # Load to Bronze layer
│   ├── ingest_bronze_enam.py
│   ├── ingest_bronze_meteo.py
│   ├── parse_bronze_agmarknet.py  # Bronze → Silver
│   ├── parse_bronze_enam.py
│   ├── parse_bronze_meteo.py
│   └── ingest_bronze_common.py    # Shared helper functions
│
├── schemas/                       # DuckDB table definitions
│   ├── 00__init_registry.sql
│   ├── 22__agmarknet_bronze_files.sql
│   ├── 23__enam_bronze_files.sql
│   ├── 41__openmeteo_silver_weather_daily.sql
│   ├── 51__agmarknet_silver_prices_daily.sql
│   ├── 61__gold_features_v1.sql
│   └── README.md
│
├── notebook/                      # Jupyter notebooks for analysis
│   ├── agmarknet.ipynb            # Explore AgMarketNet data
│   ├── enam.ipynb
│   ├── open-meteo.ipynb
│   ├── audit_enam_agmarknet.ipynb # Data quality checks
│   └── janitor.ipynb              # Data cleaning exploration
│
├── requirements.txt               # Python dependencies
├── merge.sh                       # Helper script
└── README.md                      # This file
```

---

## Getting Started

### Prerequisites
- **Python 3.8+** installed
- **5GB free disk space**
- **Stable internet connection** (for data collection)
- **Chrome browser** (for web scraping)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd crop-ai
   ```

2. **Create virtual environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On macOS/Linux
   # OR
   .venv\Scripts\activate  # On Windows
   ```

3. **Install dependencies**:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Verify installation**:
   ```bash
   python -c "import pandas, duckdb; print('Setup complete!')"
   ```

### Quick Start: See It in Action

**Step 1: Initialize database**
```bash
cat schemas/*.sql | duckdb crop-ai.duckdb
```

**Step 2: Explore sample data**
```bash
jupyter lab
# Open notebook/agmarknet.ipynb
```

**Step 3: Run a pipeline script**
```bash
python pipeline/ingest_bronze_agmarket.py
```

---

## Learning Path

### For Students (Recommended Order)

1. **Start here**: Read this README to understand the big picture
2. **Explore data**: Open `notebook/agmarknet.ipynb` to see real crop price data
3. **Set up environment**: Follow the Getting Started section to install dependencies
4. **Run a pipeline**: Execute `python pipeline/ingest_bronze_agmarket.py` to see data ingestion
5. **Dive into code**: Start with `pipeline/crawl_agmarket.py` (simplest script)
6. **Experiment**: Try modifying scripts or creating new features

### Key Concepts to Master

- [x] **Data collection**: Web scraping vs. APIs
- [ ] **Data storage**: Bronze-Silver-Gold architecture
- [ ] **Data cleaning**: Handling missing values, outliers, duplicates
- [ ] **Feature engineering**: Creating predictive variables from raw data
- [ ] **Time series**: Working with sequential data
- [ ] **Machine learning**: Training, validation, testing
- [ ] **Model evaluation**: RMSE, MAE, R² metrics
- [ ] **Reproducibility**: Version control, documentation

---

## Project Milestones

### Phase 1: Data Foundation (Completed)
- [x] Define data sources (AgMarketNet, eNAM, Open-Meteo)
- [x] Design Bronze-Silver-Gold schema
- [x] Implement web scrapers
- [x] Set up DuckDB tables
- [x] Create ingestion pipelines

### Phase 2: Data Processing (In Progress)
- [x] Bronze layer ingestion (raw data storage)
- [ ] Silver layer parsing (cleaning and standardization)
- [ ] Gold layer features (ML-ready dataset)
- [ ] Data quality validation

### Phase 3: Model Development (Planned)
- [ ] Exploratory data analysis (EDA)
- [ ] Feature engineering
- [ ] Baseline model (Linear Regression)
- [ ] XGBoost model training
- [ ] Hyperparameter tuning
- [ ] Model evaluation and comparison

### Phase 4: Insights & Presentation (Planned)
- [ ] Visualizations (price trends, predictions)
- [ ] Feature importance analysis
- [ ] Model performance report
- [ ] Final presentation deck

---

## Documentation

- **[Schema Documentation](schemas/README.md)**: DuckDB table definitions
- **[Notebooks](notebook/)**: Jupyter notebooks for data exploration and analysis

---

## Frequently Asked Questions

### General Questions

**Q: Can I use this for actual trading?**  
A: No. This is an educational project. The predictions are not financial advice and should not be used for real trading or investment decisions.

**Q: How accurate are the predictions?**  
A: Target accuracy is RMSE < 20% of mean price and R² > 0.5. This is reasonable for educational purposes but not production-grade.

**Q: Which crops are supported?**  
A: Currently focused on potato, tomato, and onion. These were chosen for data availability and economic importance.

**Q: How much historical data do we need?**  
A: Ideally 2-3 years of daily data. More data generally improves predictions, especially for capturing seasonal patterns.

### Technical Questions

**Q: Why DuckDB instead of PostgreSQL?**  
A: DuckDB is designed for analytics and runs in-process (no server setup). Perfect for learning and single-machine workflows. PostgreSQL is better for multi-user production systems.

**Q: Why XGBoost instead of neural networks?**  
A: XGBoost is faster to train, more interpretable, and performs better on small-to-medium tabular datasets. Neural networks shine with huge datasets or unstructured data (images, text).

**Q: Can I add more features?**  
A: Yes! Try adding:
- Government policy announcements
- International crop prices
- Fuel prices (affects transportation costs)
- Festival dates (demand spikes)

**Q: How do I handle missing data?**  
A: Options include:
- **Forward fill**: Use previous day's value
- **Interpolation**: Average of surrounding values
- **Mean imputation**: Replace with average
- **Remove**: Drop rows with missing values (if rare)

**Q: Why split data chronologically?**  
A: In time series, future data cannot predict the past. Random splitting would "leak" future information into training, causing overly optimistic results that don't work in reality.

---

## Contributing

This is an educational project. Contributions that improve learning value are welcome:

- **Documentation**: Clarify concepts, add examples, fix typos
- **Bug fixes**: Correct errors in code or logic
- **Visualizations**: Add charts/graphs that aid understanding
- **Experiments**: Try different models, features, or techniques
- **Tutorials**: Write guides for specific topics

---

## License

This project is for educational purposes. Please cite sources when using data from AgMarketNet, eNAM, or Open-Meteo.

---

## Acknowledgments

- **Project Mentor**:
  - **Professor K. Chelvakumar**, Visiting Professor, Mechanical Engineering, IIT Gandhinagar
  - BS: University of Peradeniya, Sri Lanka (1980)
  - MS: California Institute of Technology (1981)
  - PhD: California Institute of Technology (1985)
  - MBA: University of California, Irvine (2005)
  - Email: chelva@iitgn.ac.in

- **Data Sources**:
  - [AgMarketNet](https://agmarknet.gov.in/) — Ministry of Agriculture & Farmers Welfare, Govt. of India
  - [eNAM](https://www.enam.gov.in/) — National Agriculture Market
  - [Open-Meteo](https://open-meteo.com/) — Open-source weather API

- **Technology**:
  - [DuckDB](https://duckdb.org/) — Fast in-process analytical database
  - [XGBoost](https://xgboost.readthedocs.io/) — Gradient boosting framework
  - [Pandas](https://pandas.pydata.org/) — Data manipulation library

- **Inspiration**:
  - This project follows modern data engineering best practices (Medallion Architecture, feature stores)
  - Designed as a learning-first experience for students

---

## Support

For questions or issues:
1. Check the [FAQ section](#frequently-asked-questions) above
2. Review the code and notebooks in this repository
3. Open an issue on GitHub

---

**Remember**: The goal is learning, not perfection. Every data scientist started somewhere. Embrace the journey!

---

*Last Updated: November 11, 2025*
