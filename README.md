# nowasting_metrics
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-4-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->
Repo to automatically run metrics on the nowcasting forecast

## Metrics

### DAILY

#### MAE

- The MAE is calculated for each GSP on the latest forecast
- We calculate MAE for for all the GSPs combined on the latest forecast
- We also calculate MAE for different forecast horizon, from 0 to 8 hours, for the national forecast.
- The MAE for PVLive initial and update estimate is also calculated for all GSPs

#### RMSE

- The RMSE is calculated for each GSP on the latest forecast
- We calculate RMSE for for all the GSPs combined on the latest forecast
- We also calculate RMSE for different forecast horizon, from 0 to 8 hours, for the national forecast.
- The RMSE for PVLive initial and update estimate is also calculated for all GSPs

### ME

- The ME is calculated for National only from the last week. It is grouped by `time of day` and `forecast_horizon`. 

### Ramp Rate

- The MAE of the ramp rate is calculated for each model for the national forecast only. 
We define ramp rate as
`((pred_{t+1 hour} - pred_{t}) - (true_{t+1 hour} - true_{t}))` for each individual forecast run. 
We calculate the 0, 1 and 2 hour ramp rate. The 0 hour ramp rate looks at values at 0 and 1 hour from each forecast. 

### Probabilistic

- We look at the average pinball loss and the average exceedance for plevels 10 and 90. 
- This for `PVnet` and `National_xg`
- We look at different forecast horizons for this metric


## Tests
### Local pytest

To run local pytests you need to
1. add `src` to python path `export PYTHONPATH=$PYTHONPATH:./nowcasting_metrics`
3. run pytests: `pytest`


### Docker Tests

TO run tests use the following command
```bash
docker stop $(docker ps -a -q)
docker-compose -f test-docker-compose.yml build
docker-compose -f test-docker-compose.yml run nowcasting_metrics
```

## Running the app
### Environmental Variables
The environmental variables are
DB_URL: The database url you want to save the results to
N_GSPS: The number of gsps you want to pull
DATETIME_NOW: The datetime of when this app is ran. Default is None, and Now() is selected.
This is useful as the app calculates the daily metrics from yesterday
USE_PVNET_GSP_SUM: Option to use `pvnet_gsp_sum` or not. Default is false

These options can also be enter like this:


### Run Locally
First add 'nowcasting_metrics' to your python path:
```
export PYTHONPATH=$PYTHONPATH:./nowcasting_metrics
```
Then run the app.
```
python nowcasting_metrics/app.py --n-gsps=10
```
You will need to set 'DB_URL'

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/abhijelly"><img src="https://avatars.githubusercontent.com/u/75399048?v=4?s=100" width="100px;" alt="Abhijeet"/><br /><sub><b>Abhijeet</b></sub></a><br /><a href="https://github.com/openclimatefix/nowcasting-metrics/commits?author=abhijelly" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/peterdudfield"><img src="https://avatars.githubusercontent.com/u/34686298?v=4?s=100" width="100px;" alt="Peter Dudfield"/><br /><sub><b>Peter Dudfield</b></sub></a><br /><a href="https://github.com/openclimatefix/nowcasting-metrics/commits?author=peterdudfield" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/AnshRoshan"><img src="https://avatars.githubusercontent.com/u/71877143?v=4?s=100" width="100px;" alt="Ansh Roshan"/><br /><sub><b>Ansh Roshan</b></sub></a><br /><a href="https://github.com/openclimatefix/nowcasting-metrics/commits?author=AnshRoshan" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/aarxshi"><img src="https://avatars.githubusercontent.com/u/132636356?v=4?s=100" width="100px;" alt="Aarushi Chakraborty"/><br /><sub><b>Aarushi Chakraborty</b></sub></a><br /><a href="https://github.com/openclimatefix/nowcasting-metrics/commits?author=aarxshi" title="Code">💻</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!