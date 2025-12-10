# 3. Health checks: Add actuator (e.g.  /health)  endpoints to coordinator and shards. Also add endpoint returning build version. (2 points)

 
## Додаємо методи 
![3.2.png](./3.2.png)
![3.3.png](./3.3.png)

## Перезбираємо образи
![3.4.png](./3.4.png)

## Перезапускаємо Kubernetes
![3.5.png](./3.5.png)

## Скріншот /health
![3.1.png](./3.1.png)

## Скріншот /version
![3.6.png](./3.6.png)

## Згідно з вимогами, додано ендпоінт /version, який повертає поточну версію мікросервісу та дату збірки. Це корисно для CI/CD процесів, щоб переконатися, що в кластері розгорнуто правильну версію коду. 



