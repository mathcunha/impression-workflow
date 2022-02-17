# Engineering challenge

## How it works?

The [workflow](./workflow.py) reads 2 lists of file with click and impression events.

```
python workflow.py -i resources/impressions.json resources/impressions2.json -c resources/clicks.json
```

The example above reads two impression files (e.g. [impressions.json](./resources/impressions.json)) and one click file (i.e. [clicks.json](./resources/clicks.json)).
After reading the files, the workflow will calculate some metrics and save that at output_app_id_country_metrics.json and output_app_id_country_top_advertisers.json.

## Run

### Local execution
- Install depencies presented at [requirements.txt](./requirements.txt)
- Run the command bellow:
```
python workflow.py -i resources/impressions.json -c resources/clicks.json
```
### Container execution
```
docker-compose up
```
### Results
Once the execution is finished, see results:
  - Calculated metrics: output_app_id_country_metrics.json
  - Top 5 advertisers: output_app_id_country_top_advertisers.json
  
## Data pipeline

The [workflow.py](./workflow.py) is responsable for orchestrate the data pipeline, which consists in read the input files, delete duplicated impressions, attribute clicks to impressions, calculate metrics.

The reading phase starts 2 threads in [parallel](./workflow.py#L67) to process impressions and clicks events. One thread run the method [read_impressions](./workflow.py#L14) and the other run the method [read_clicks](./workflow.py#L23). These methods call others tasks defined at module [tasks.py](./tasks.py). After reads de input data, the workflow deletes duplicated impressions and calls method [conversions](./workflow.py#L29) to joins clicks to attribute clicks to impressions.

The final stage of the pipeline is the output files generation. Workflow calls task [calculate_metrics](./workflow.py#L36) to calculate some metrics, and [recommend_advertisers](./workflow.py#L46) to identify which are the top advertisers.
