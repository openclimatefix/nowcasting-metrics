# nowasting_metrics
Repo to automatically run metrics on the nowcasting forecast


### Local pytest

To run local pytests you need to
1. add `src` to python path `export PYTHONPATH=$PYTHONPATH:./nowcasting_metrics`
3. run pytests: `pytest`


### Docker Tests

TO run tests use the following command
```bash
docker-compose -f test-docker-compose.yml build
docker-compose -f test-docker-compose.yml run nowcasting_metrics
```
