---
title: "SQL Mastery for Data Engineers: Phase 2, Intermediate to Advanced"
description: "Apply advanced SQL patterns for real-world data engineering transformations."
weight: 2
---

# SQL Mastery for Data Engineers: Phase 2, Intermediate to Advanced

*Estimated time: 2-4 weeks | Level: Intermediate | Database used in examples: PostgreSQL*

Phase 1 gave you trustworthy fundamentals. Phase 2 gives you leverage. The techniques here, especially window functions, are what separate someone who can *get* an answer from someone who can build clean, fast, maintainable transformations. They appear in almost every real pipeline: deduplicating raw event feeds, computing running totals, detecting sessions, finding gaps in data, and comparing a row to its neighbors.

This post covers:

1. Window functions (the big one)
2. Deduplication patterns
3. Gaps and islands
4. Sessionization
5. Set operators: `UNION`, `INTERSECT`, `EXCEPT`
6. `EXISTS` vs `IN`, revisited
7. Set-based thinking

## The sample data

We will extend the Phase 1 schema with a few new tables. Paste this into PostgreSQL to follow along.

```sql
CREATE TABLE orders (
    order_id     INT PRIMARY KEY,
    customer_id  INT,
    order_date   DATE,
    status       TEXT,
    amount       NUMERIC(10,2)
);

INSERT INTO orders VALUES
 (101, 1, '2024-02-01', 'completed', 120.00),
 (102, 1, '2024-03-12', 'completed',  80.00),
 (103, 2, '2024-03-15', 'cancelled',  50.00),
 (104, 3, '2024-04-02', 'completed', 200.00),
 (105, 3, '2024-04-20', 'completed',  60.00),
 (106, 4, '2024-05-01', 'pending',    NULL),
 (107, 1, '2024-05-10', 'completed', 150.00),
 (108, 3, '2024-05-18', 'completed',  90.00);

-- A raw event feed, with duplicates, as you would see from an ingestion job
CREATE TABLE raw_events (
    event_id    INT,
    user_id     INT,
    event_type  TEXT,
    event_time  TIMESTAMP,
    loaded_at   TIMESTAMP
);

INSERT INTO raw_events VALUES
 (1, 10, 'login',    '2024-06-01 09:00', '2024-06-01 10:00'),
 (1, 10, 'login',    '2024-06-01 09:00', '2024-06-01 11:00'),  -- duplicate reload
 (2, 10, 'view',     '2024-06-01 09:05', '2024-06-01 10:00'),
 (3, 10, 'view',     '2024-06-01 09:20', '2024-06-01 10:00'),
 (4, 10, 'purchase', '2024-06-01 11:30', '2024-06-01 12:00'),
 (5, 10, 'view',     '2024-06-01 11:40', '2024-06-01 12:00'),
 (6, 20, 'login',    '2024-06-01 09:10', '2024-06-01 10:00'),
 (7, 20, 'view',     '2024-06-01 09:12', '2024-06-01 10:00');

-- Daily activity, with gaps
CREATE TABLE daily_activity (
    user_id        INT,
    activity_date  DATE
);

INSERT INTO daily_activity VALUES
 (1, '2024-07-01'), (1, '2024-07-02'), (1, '2024-07-03'),
 (1, '2024-07-06'), (1, '2024-07-07'),
 (1, '2024-07-10');
```

## 1. Window functions

### The core idea

An aggregate with `GROUP BY` collapses rows. A **window function** computes a value across a set of related rows *without collapsing them*. Every input row stays in the output, and gains a new column calculated from its "window" of neighbors.

Compare the two:

```sql
-- GROUP BY: one row per customer
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id;

-- Window: every order row, with the customer total alongside it
SELECT order_id, customer_id, amount,
       SUM(amount) OVER (PARTITION BY customer_id) AS customer_total
FROM orders;
```

The second query lets you do things like "what percent of the customer's total is this order?" in one pass, with no self join.

### Anatomy of a window

```sql
function() OVER (
    PARTITION BY ...   -- split rows into independent groups (optional)
    ORDER BY ...       -- order rows within each partition (often required)
    ROWS/RANGE ...     -- frame: which rows around the current row to include (optional)
)
```

- **`PARTITION BY`** works like `GROUP BY` for the window: the calculation restarts for each partition. Omit it and the whole result set is one partition.
- **`ORDER BY`** inside `OVER` defines the sequence the function sees. It is separate from the query's final `ORDER BY`.
- **The frame** says which rows relative to the current one are included. We will cover this shortly.

**Execution order note:** window functions are evaluated after `WHERE`, `GROUP BY`, and `HAVING`, but before the final `ORDER BY` and `LIMIT`. So you cannot filter on a window function result directly in `WHERE`. You wrap it in a subquery or CTE instead.

### Ranking functions

These number rows within a partition.

```sql
SELECT customer_id, order_id, amount,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn,
       RANK()       OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk,
       DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS drnk
FROM orders
WHERE amount IS NOT NULL;
```

The difference lies in how ties are handled:

- **`ROW_NUMBER()`** gives every row a unique number (1, 2, 3, 4). Ties are broken arbitrarily unless your `ORDER BY` is deterministic.
- **`RANK()`** gives tied rows the same rank and then skips numbers (1, 2, 2, 4).
- **`DENSE_RANK()`** gives tied rows the same rank without skipping (1, 2, 2, 3).

Two related functions are worth knowing: `NTILE(n)` splits rows into `n` roughly equal buckets (quartiles, deciles), and `PERCENT_RANK()` returns relative standing between 0 and 1.

**The top-N-per-group pattern** is one of the most common uses. Since you cannot filter on a window function in `WHERE`, wrap it:

```sql
-- Each customer's single largest order
WITH ranked AS (
    SELECT customer_id, order_id, amount,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY amount DESC, order_id   -- tiebreaker keeps it deterministic
           ) AS rn
    FROM orders
    WHERE amount IS NOT NULL
)
SELECT customer_id, order_id, amount
FROM ranked
WHERE rn = 1;
```

Change `rn = 1` to `rn <= 3` for the top three per group. Always add a tiebreaker column to the `ORDER BY` so results are reproducible between runs.

### Navigation functions: LAG and LEAD

`LAG` looks at a previous row, and `LEAD` looks at a following one, within the partition and ordering you specify. This replaces the old self join trick for comparing a row to its neighbor.

```sql
SELECT customer_id, order_date, amount,
       LAG(order_date)  OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date,
       LAG(amount, 1, 0) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount,
       order_date - LAG(order_date) OVER (
           PARTITION BY customer_id ORDER BY order_date
       ) AS days_since_prev_order
FROM orders
ORDER BY customer_id, order_date;
```

`LAG(amount, 1, 0)` means "look back 1 row, and return 0 if there is no such row". The first row of each partition has no previous row, so without a default you get `NULL`.

To avoid repeating the same window definition, name it with the `WINDOW` clause:

```sql
SELECT customer_id, order_date,
       LAG(order_date) OVER w AS prev_date,
       LEAD(order_date) OVER w AS next_date
FROM orders
WINDOW w AS (PARTITION BY customer_id ORDER BY order_date);
```

Two more related functions are `FIRST_VALUE()` and `LAST_VALUE()`. Be careful with `LAST_VALUE`, which has a famous trap covered in the frames section below.

### Aggregate window functions and running totals

Any aggregate (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`) can be used as a window function by adding `OVER`. When you add an `ORDER BY` inside the window, you typically get a **running** calculation:

```sql
SELECT customer_id, order_date, amount,
       SUM(amount) OVER (
           PARTITION BY customer_id
           ORDER BY order_date
       ) AS running_total
FROM orders
WHERE amount IS NOT NULL
ORDER BY customer_id, order_date;
```

For customer 1 this produces 120, then 200, then 350, each row adding to the total so far.

### Window frames: ROWS, RANGE, and the LAST\_VALUE trap

The **frame** controls exactly which rows around the current one feed the calculation. The general syntax is:

```sql
ROWS BETWEEN <start> AND <end>
```

where the boundaries can be `UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`, `n FOLLOWING`, or `UNBOUNDED FOLLOWING`.

A moving average over the current and previous two orders:

```sql
SELECT customer_id, order_date, amount,
       AVG(amount) OVER (
           PARTITION BY customer_id
           ORDER BY order_date
           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       ) AS moving_avg_3
FROM orders
WHERE amount IS NOT NULL;
```

**ROWS vs RANGE.** `ROWS` counts physical rows. `RANGE` works on *values*, so all rows that tie on the `ORDER BY` value (peers) are treated as a group. This difference matters when your ordering column has duplicates.

**The default frame is a trap.** When you specify an `ORDER BY` in the window but no explicit frame, the default is:

```sql
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

That is why `SUM(...) OVER (ORDER BY ...)` gives a running total. It also explains the `LAST_VALUE` trap: with the default frame, "the last value" is just the current row (or its peers), because the frame ends at the current row. To get the true last value of the partition, extend the frame:

```sql
SELECT customer_id, order_date,
       LAST_VALUE(amount) OVER (
           PARTITION BY customer_id
           ORDER BY order_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS last_amount
FROM orders;
```

When in doubt, state the frame explicitly. It makes the intent obvious and avoids surprises.

### Practical window patterns

**Percent of total:**

```sql
SELECT customer_id, order_id, amount,
       ROUND(100.0 * amount / SUM(amount) OVER (PARTITION BY customer_id), 1) AS pct_of_customer
FROM orders
WHERE amount IS NOT NULL;
```

**Month-over-month change:**

```sql
WITH monthly AS (
    SELECT DATE_TRUNC('month', order_date) AS month,
           SUM(amount) AS revenue
    FROM orders
    WHERE status = 'completed'
    GROUP BY 1
)
SELECT month, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
             / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 1) AS pct_change
FROM monthly
ORDER BY month;
```

Notice the layering: aggregate with `GROUP BY` in a CTE first, then apply window functions on top. This two-step shape is extremely common.

**Cumulative distinct counts** are *not* directly supported with `COUNT(DISTINCT ...) OVER`. A reliable workaround is to flag each user's first occurrence with `ROW_NUMBER()` and then take a running sum of that flag.

### Practice for this section

- Rank customers by total spend within each country.
- Return the second-largest order for each customer.
- For each order, show the days since the customer's previous order, then find the customer with the shortest average gap.
- Compute a 7-day rolling sum of daily revenue. (Hint: build a complete date spine first, otherwise missing days distort `ROWS`-based frames.)
- Explain why a running total and a rolling 3-row average use different frames.

## 2. Deduplication patterns

Duplicates are a fact of life in data engineering. Retried API calls, at-least-once message delivery, reloaded files, and overlapping extracts all produce them. Removing them correctly means deciding **which copy to keep**, not just collapsing identical rows.

### The `DISTINCT` shortcut and its limits

`SELECT DISTINCT` removes rows that are identical across *all* selected columns. It works when duplicates are exact copies. It fails when copies differ slightly (for instance, a different `loaded_at` timestamp), and it gives you no say in which version survives.

### The ROW\_NUMBER pattern (the workhorse)

Partition by the business key, order by whatever defines "best" (usually the latest load), and keep row 1:

```sql
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY event_id
               ORDER BY loaded_at DESC
           ) AS rn
    FROM raw_events
)
SELECT event_id, user_id, event_type, event_time, loaded_at
FROM ranked
WHERE rn = 1;
```

This keeps the most recently loaded version of each event. The two decisions you must make deliberately are:

- **The partition key.** What defines "the same record"? Here it is `event_id`. If you choose it wrong, you delete real data or keep real duplicates.
- **The ordering.** Which copy wins? Latest load, highest version number, or non-null values preferred.

Add a final tiebreaker to the `ORDER BY` if two copies could share the same `loaded_at`, otherwise your result is nondeterministic.

### PostgreSQL shortcut: DISTINCT ON

PostgreSQL offers a compact alternative:

```sql
SELECT DISTINCT ON (event_id) *
FROM raw_events
ORDER BY event_id, loaded_at DESC;
```

It returns the first row per `event_id` according to the `ORDER BY`. It is concise but non-standard, so it will not work in other databases. The `ROW_NUMBER` pattern ports everywhere. Some warehouses (Snowflake, BigQuery) also offer a `QUALIFY` clause that filters on window functions directly, removing the need for the wrapper CTE:

```sql
-- Snowflake / BigQuery style (not PostgreSQL)
SELECT * FROM raw_events
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY loaded_at DESC) = 1;
```

### Finding duplicates before removing them

Always look before you delete:

```sql
SELECT event_id, COUNT(*) AS copies
FROM raw_events
GROUP BY event_id
HAVING COUNT(*) > 1;
```

### Deleting duplicates in place

If you must physically remove them (in a table you own), use a similar window approach against a unique row identifier. In PostgreSQL the system column `ctid` can serve as one:

```sql
DELETE FROM raw_events
WHERE ctid IN (
    SELECT ctid FROM (
        SELECT ctid,
               ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY loaded_at DESC) AS rn
        FROM raw_events
    ) t
    WHERE rn > 1
);
```

In production pipelines it is usually safer to build a *clean* table or view from the raw data instead of deleting from the raw layer. Keeping raw data immutable lets you reprocess when your dedup logic changes.

### Practice for this section

- Deduplicate `raw_events` keeping the earliest load instead of the latest.
- Deduplicate by a composite key (`user_id` plus `event_type` plus `event_time`).
- Write a data quality query that reports every key with more than one copy and how many.
- Explain when `DISTINCT` is wrong and `ROW_NUMBER` is right.

## 3. Gaps and islands

This is a classic problem family with two faces:

- **Islands:** find consecutive runs of values (a streak of daily logins).
- **Gaps:** find the missing values between runs (days with no activity).

### The islands trick

Consider `daily_activity` for user 1: July 1-3, then 6-7, then 10. We want to identify each streak and its length.

The key insight: if you subtract a row number from a sequential value, **consecutive values produce a constant**. That constant becomes a group identifier.

```sql
WITH numbered AS (
    SELECT user_id, activity_date,
           activity_date - (ROW_NUMBER() OVER (
               PARTITION BY user_id ORDER BY activity_date
           ))::int AS grp
    FROM daily_activity
)
SELECT user_id,
       MIN(activity_date) AS streak_start,
       MAX(activity_date) AS streak_end,
       COUNT(*)           AS streak_days
FROM numbered
GROUP BY user_id, grp
ORDER BY streak_start;
```

Walk through why it works: for July 1, 2, 3 the row numbers are 1, 2, 3, so date minus row number is the same value each time. When the sequence jumps to July 6 (row number 4), the result changes, which starts a new group. Each distinct `grp` is one island.

The output is three streaks: July 1-3 (3 days), July 6-7 (2 days), and July 10 (1 day).

If your data could contain duplicate dates for a user, deduplicate first (using the pattern from section 2), or the row-number arithmetic breaks.

### Finding the gaps

To find the missing stretches between activity, use `LEAD` to compare each date to the next one:

```sql
WITH next_dates AS (
    SELECT user_id, activity_date,
           LEAD(activity_date) OVER (
               PARTITION BY user_id ORDER BY activity_date
           ) AS next_date
    FROM daily_activity
)
SELECT user_id,
       activity_date + 1  AS gap_start,
       next_date - 1      AS gap_end,
       next_date - activity_date - 1 AS missing_days
FROM next_dates
WHERE next_date - activity_date > 1;
```

This returns July 4-5 (2 missing days) and July 8-9 (2 missing days).

### Where you will use this

- Detecting streaks (login streaks, consecutive trading days).
- Finding outages or missing data in time series.
- Grouping consecutive status periods in slowly changing data.
- Validating that a pipeline delivered every expected day.

### Practice for this section

- Find the longest activity streak for each user.
- List all dates in July with no activity, using a date spine and an anti-join.
- Group consecutive order IDs into ranges (for example, 101-103, 105-108).

## 4. Sessionization

Raw event logs usually have no concept of a "session". You have to construct one. The standard rule is: *a new session starts when the gap since the user's previous event exceeds a threshold* (commonly 30 minutes).

The pattern combines two window steps:

1. Use `LAG` to compute the time since the previous event, and flag session starts.
2. Use a running `SUM` of those flags to assign a session number.

```sql
WITH deduped AS (
    SELECT DISTINCT ON (event_id) *
    FROM raw_events
    ORDER BY event_id, loaded_at DESC
),
flagged AS (
    SELECT *,
           CASE
               WHEN LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) IS NULL
                 OR event_time - LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time)
                      > INTERVAL '30 minutes'
               THEN 1 ELSE 0
           END AS is_new_session
    FROM deduped
),
sessions AS (
    SELECT *,
           SUM(is_new_session) OVER (
               PARTITION BY user_id ORDER BY event_time
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS session_id
    FROM flagged
)
SELECT user_id, session_id,
       MIN(event_time) AS session_start,
       MAX(event_time) AS session_end,
       COUNT(*)        AS events
FROM sessions
GROUP BY user_id, session_id
ORDER BY user_id, session_id;
```

For user 10, the events at 09:00, 09:05, and 09:20 form session 1. The next event at 11:30 is more than 30 minutes later, so it starts session 2, which includes the 11:40 view.

Notice how this builds on everything so far: deduplication first, then `LAG`, then `CASE`, then a running sum, then a final `GROUP BY`. This kind of layered CTE pipeline is the daily bread of analytics engineering.

**Extensions worth trying:** compute session duration, count events per session, find the first and last event type in each session using `FIRST_VALUE` and `LAST_VALUE` with a full frame, and build a simple conversion funnel (login, view, purchase) per session.

### Practice for this section

- Change the session threshold to 10 minutes and see how the sessions change.
- Compute the average number of events per session.
- Find sessions that include a purchase.

## 5. Set operators: UNION, INTERSECT, EXCEPT

Where joins combine tables *horizontally* (adding columns), set operators combine query results *vertically* (stacking rows). Both queries must return the same number of columns with compatible types.

### UNION and UNION ALL

```sql
SELECT customer_id FROM orders WHERE status = 'completed'
UNION
SELECT customer_id FROM orders WHERE status = 'pending';
```

- **`UNION`** removes duplicate rows from the combined result. This requires a sort or hash step and costs extra.
- **`UNION ALL`** keeps everything, including duplicates, and is faster.

**Default to `UNION ALL`** unless you specifically need deduplication. Using plain `UNION` out of habit is a common and easily avoided performance drain. `UNION ALL` is also the standard way to combine same-shaped tables, such as monthly partitions or data from multiple source systems.

### INTERSECT

Returns only rows present in *both* results:

```sql
-- Customers with both completed and cancelled orders
SELECT customer_id FROM orders WHERE status = 'completed'
INTERSECT
SELECT customer_id FROM orders WHERE status = 'cancelled';
```

### EXCEPT

Returns rows from the first result that are *not* in the second (called `MINUS` in Oracle):

```sql
-- Customers with completed orders but never any cancelled ones
SELECT customer_id FROM orders WHERE status = 'completed'
EXCEPT
SELECT customer_id FROM orders WHERE status = 'cancelled';
```

### Useful properties and gotchas

- `INTERSECT` and `EXCEPT` treat `NULL`s as **equal** to each other for the purpose of comparison, unlike `=` in a join. This can actually be handy.
- `EXCEPT` is order-dependent: `A EXCEPT B` is not the same as `B EXCEPT A`.
- Column names in the result come from the **first** query.
- Column *position* matters, not name. Mismatched order silently produces wrong results if the types happen to be compatible.

### The data engineering use: reconciliation

`EXCEPT` shines for comparing two datasets, such as a source table and its warehouse copy after a migration:

```sql
-- Rows in source missing from target
SELECT * FROM source_table
EXCEPT
SELECT * FROM target_table;

-- Rows in target that should not be there
SELECT * FROM target_table
EXCEPT
SELECT * FROM source_table;
```

If both queries return zero rows, the tables match exactly (as sets). This is a quick, powerful validation check.

### Practice for this section

- Combine two monthly order tables with `UNION ALL`, then try plain `UNION` and compare row counts.
- Find users active in both June and July.
- Write a reconciliation query that reports differences in both directions.

## 6. EXISTS vs IN, revisited

You met this briefly in Phase 1. Here is the fuller picture now that you have more tools.

**`IN`** compares a value against a list produced by a subquery:

```sql
SELECT * FROM customers
WHERE customer_id IN (SELECT customer_id FROM orders WHERE status = 'completed');
```

**`EXISTS`** asks "does at least one matching row exist?":

```sql
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.customer_id AND o.status = 'completed'
);
```

How to choose:

- **For positive checks**, `IN` and `EXISTS` usually perform equally on modern optimizers, so pick for readability.
- **For negative checks, always use `NOT EXISTS`** rather than `NOT IN`. If the subquery can return even one `NULL`, `NOT IN` returns no rows at all, which is a classic silent bug.
- **`EXISTS` stops at the first match** and does not care how many matches there are, so it also avoids row multiplication that a join would cause. When you only need to *filter* by the existence of related rows, `EXISTS` is often cleaner than a join plus `DISTINCT`.
- **`EXISTS` handles multiple correlation conditions** more naturally than `IN`, which compares only a single column (or a tuple).

The select list inside `EXISTS` is irrelevant. `SELECT 1` is a common convention.

**Semi-join and anti-join** are the formal names for these patterns: a semi-join keeps rows that have a match (`EXISTS`), and an anti-join keeps rows that have none (`NOT EXISTS`). You will hear these terms constantly when reading execution plans.

## 7. Set-based thinking

The hardest part of this phase is not syntax. It is a shift in mindset.

Procedural languages train you to think: *for each row, do this*. SQL works best when you instead describe the **entire result you want**, and let the engine work out how to produce it. This is called set-based thinking.

### Row-by-row vs set-based

Suppose you want to flag each order as above or below its customer's average. A procedural mind reaches for a loop: fetch each customer, compute an average, then update each order. The set-based answer is a single statement:

```sql
SELECT order_id, customer_id, amount,
       CASE WHEN amount > AVG(amount) OVER (PARTITION BY customer_id)
            THEN 'above' ELSE 'below_or_equal' END AS vs_avg
FROM orders
WHERE amount IS NOT NULL;
```

The database can parallelize, optimize, and use indexes for the set-based version in ways it simply cannot for a loop of row-level calls.

### Habits that build the mindset

- **Ask "what is the grain?"** before you write anything. What does one row of the output represent?
- **Think in transformations, not steps.** Describe how the output relates to the input, then express that as joins, windows, and aggregates.
- **Prefer one statement over many.** Avoid cursors and loops. When you see a procedural loop in SQL, there is almost always a set-based rewrite.
- **Layer with CTEs.** Each CTE should do one conceptual thing: dedupe, then enrich, then aggregate.
- **Avoid row-by-row functions on large tables** in `SELECT` or `WHERE` where a join or window would do the job in bulk.
- **Push filters early.** Reduce the data as soon as you can, before expensive joins and windows.

### Common anti-patterns to unlearn

- Correlated subqueries that run per row when a window function or join would do.
- Using `DISTINCT` to cover up an unexamined join explosion.
- Repeating the same expression many times instead of computing it once in a CTE.
- Updating or inserting rows one at a time from application code, when one `INSERT ... SELECT` or `UPDATE ... FROM` would do.

### Practice for this section

- Take a correlated subquery you wrote in Phase 1 and rewrite it with a window function. Compare the execution plans with `EXPLAIN ANALYZE`.
- Rewrite a multi-step loop idea (for example, "update every customer's tier based on spend") as a single `UPDATE ... FROM` with a CTE.

## A suggested 3-week plan

**Week 1: Window functions**

- Days 1-2: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, and top-N-per-group.
- Days 3-4: `LAG`, `LEAD`, running totals, and percent of total.
- Days 5-6: Frames, `ROWS` vs `RANGE`, moving averages, and the `LAST_VALUE` trap.
- Day 7: 15-20 practice problems from LeetCode, StrataScratch, or DataLemur.

**Week 2: Applied patterns**

- Days 1-2: Deduplication, using `ROW_NUMBER` and `DISTINCT ON`.
- Days 3-4: Gaps and islands.
- Days 5-6: Sessionization.
- Day 7: Review and redo the hardest problems from memory.

**Week 3: Sets and mindset**

- Days 1-2: `UNION`, `INTERSECT`, `EXCEPT`, and reconciliation queries.
- Day 3: `EXISTS` vs `IN`, semi-joins and anti-joins.
- Days 4-5: Rewrite old queries set-based, and compare execution plans.
- Days 6-7: **Capstone.** Take a raw clickstream or transaction dataset and build a clean pipeline: deduplicate, sessionize, compute running metrics, and produce a reconciliation check, all as a chain of CTEs.

## Common mistakes checklist

- Did I add a deterministic tiebreaker to every `ROW_NUMBER` ordering?
- Am I trying to filter on a window function in `WHERE` instead of a wrapper CTE?
- Do I understand which frame my window is using, especially for `LAST_VALUE`?
- Is my date spine complete before applying `ROWS`-based rolling windows?
- Did I deduplicate before applying gaps-and-islands arithmetic?
- Am I using `UNION` where `UNION ALL` would do?
- Am I using `NOT IN` against a column that may contain `NULL`?
- Does my set operator have the same column order in both queries?
- Could this be expressed as one set-based statement rather than a loop?

## Wrapping up

If Phase 1 taught you to trust your queries, Phase 2 teaches you to make them *elegant*. Window functions in particular will change how you write SQL: problems that once needed self joins, correlated subqueries, or procedural code collapse into a few readable lines.

By the end of this phase you should be able to deduplicate any messy feed, detect streaks and gaps, build sessions from raw events, compare datasets with set operators, and reach for the set-based solution first.

With these in hand you are ready for the next stage, where SQL meets the realities of data engineering at scale: data modeling, performance tuning, and incremental loads.

*Next up: Phase 3, Data Engineering Essentials.*
