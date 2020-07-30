This project is developed for data synchronization between Hive(ORC format data) to MySQL.

## Feature
* download source ORC file from object storage(etc. Aliyun OSS)
* auto create MySQL table
* parse ORC file and write to Mysql
* sync tasks scheduled by airflow
* totally support kubernetes deploy
* task log write to remote(Aliyun OSS)

## Workflow
![workflow](https://pics.lxkaka.wang/oss_sync_flow.png)

## Architecture
![arch](https://pics.lxkaka.wang/airflow_k8s.png)

## Task Example
![task](https://pics.lxkaka.wang/oss_dag.png)
