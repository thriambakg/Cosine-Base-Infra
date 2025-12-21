"""
Analysis: Does reducing concurrency help process more filings before hitting 25k event limit?

Step Functions Event Breakdown:
- Each Lambda invocation creates ~6 events:
  * ExecutionStarted
  * TaskStateEntered
  * TaskScheduled
  * TaskStarted
  * TaskSucceeded (or TaskFailed)
  * TaskStateExited

- Each Map state creates overhead:
  * MapStateEntered
  * MapStateStarted
  * MapIterationStarted (per item)
  * MapIterationSucceeded (per item)
  * MapStateExited

- Retry attempts add additional events

Let's calculate for different concurrency levels:
"""

def calculate_events(total_pages, batch_size, concurrency):
    """
    Calculate total events for processing total_pages with given batch_size and concurrency.
    
    Args:
        total_pages: Total number of pages to process
        batch_size: Pages per batch
        concurrency: MaxConcurrency for Map state
    """
    num_batches = (total_pages + batch_size - 1) // batch_size
    
    # Events per Lambda invocation
    events_per_lambda = 6  # Start, Entered, Scheduled, Started, Succeeded, Exited
    
    # Events per batch (Map state)
    map_overhead = 5  # Entered, Started, Exited, plus iteration start/end overhead
    events_per_batch = (batch_size * events_per_lambda) + map_overhead
    
    # Outer Map state (for batches)
    outer_map_overhead = 5
    total_events = (num_batches * events_per_batch) + outer_map_overhead
    
    # Add fetcher and other states
    other_states_events = 20  # Fetcher, Success, etc.
    total_events += other_states_events
    
    return {
        'total_pages': total_pages,
        'batch_size': batch_size,
        'concurrency': concurrency,
        'num_batches': num_batches,
        'events_per_batch': events_per_batch,
        'total_events': total_events,
        'max_pages_before_limit': int((25000 - other_states_events) / (events_per_lambda + (map_overhead / batch_size)))
    }

# Scenario: Processing 2.5 million records = 100,000 pages (25 records per page)
total_pages = 100000

print("=" * 80)
print("Step Functions Event Analysis: Concurrency vs Event Limit")
print("=" * 80)
print(f"\nScenario: Processing {total_pages:,} pages (2.5M records @ 25/page)\n")

# Test different configurations
configs = [
    {'batch_size': 25, 'concurrency': 25, 'name': 'Original (25 parallel)'},
    {'batch_size': 10, 'concurrency': 10, 'name': 'Reduced (10 parallel)'},
    {'batch_size': 5, 'concurrency': 5, 'name': 'Very Low (5 parallel)'},
]

for config in configs:
    result = calculate_events(total_pages, config['batch_size'], config['concurrency'])
    print(f"\n{config['name']}:")
    print(f"  Batch size: {result['batch_size']}")
    print(f"  Concurrency: {result['concurrency']}")
    print(f"  Number of batches: {result['num_batches']:,}")
    print(f"  Events per batch: ~{result['events_per_batch']}")
    print(f"  Total events: ~{result['total_events']:,}")
    print(f"  Exceeds 25k limit: {'YES ❌' if result['total_events'] > 25000 else 'NO ✅'}")
    
    # Calculate how many pages we can process before hitting limit
    max_pages = int((25000 - 20) / (6 + (5 / result['batch_size'])))
    print(f"  Max pages before limit: ~{max_pages:,} ({max_pages * 25:,} records)")

print("\n" + "=" * 80)
print("CONCLUSION:")
print("=" * 80)
print("""
Reducing concurrency does NOT help process more filings before hitting the limit.
The total events are roughly the same because:
- Fewer parallel executions = fewer events per batch
- But more batches = more Map state overhead events
- Net result: Similar total events for same number of pages

SOLUTIONS:
1. Process fewer pages per execution (split into multiple Step Function runs)
2. Use Glue job (no event limit, but slower)
3. Accept limit and use exporter Lambda to save progress/resume
4. Process in chunks: Run Step Function multiple times with date ranges
""")



