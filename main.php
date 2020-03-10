<?php

include "vendor/autoload.php";

$redis = new RedisDataSource\RedisDataSource();
$redis->setLocation("users2");
$redis->setup();
$res = $redis
        ->where('id',2)
        ->getMany();
var_dump($res);
#$redis = new Redis();
#$redis->connect('127.0.0.1');

#var_dump($redis->hset('hash',1,json_encode(['builder','gghhhg','ffff'])));
#var_dump($redis->hget('hash',1));

//var_dump($redis->get('bob'));

