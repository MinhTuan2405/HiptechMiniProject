up:
	docker-compose up -d

down:
	docker-compose down

restart:
	make down && make up

build:
	docker-compose build