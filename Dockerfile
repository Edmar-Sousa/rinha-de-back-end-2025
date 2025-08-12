FROM phpswoole/swoole:php8.4-alpine


WORKDIR /api

COPY . .

RUN composer install

EXPOSE 9501

CMD ["php", "src/server.php"]
