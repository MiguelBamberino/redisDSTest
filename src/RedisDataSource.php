<?php
namespace RedisDataSource;
use PPCore\Adapters\DataSources\DataSourceInterface;
use PPCore\Adapters\DataSources\AbstractDataSource;

class RedisDataSource extends AbstractDataSource{
  
  private $server;
  private $cursor=-1;
  private $pkeys = [1,2,3,4,5];
  private $primary_key='id';
  
  public function __construct(){
    $this->server = new \Redis();
    $this->server->connect('127.0.0.1');
  }
  
  public function setup(){
    $loc = $this->getLocation();
    var_dump($loc);
    $this->server->del($loc);
    $this->server->hset($loc,1,json_encode(['id'=>1,'name'=>'bob','role'=>'a']));
    $this->server->hset($loc,2,json_encode(['id'=>2,'name'=>'terry','role'=>'d']));
    $this->server->hset($loc,3,json_encode(['id'=>3,'name'=>'frank','role'=>'a']));
    $this->server->hset($loc,4,json_encode(['id'=>4,'name'=>'jordi','role'=>'v']));
    $this->server->hset($loc,5,json_encode(['id'=>5,'name'=>'jordi','role'=>'v']));
    $this->server->hset($loc,6,json_encode(['id'=>6,'name'=>'jordi','role'=>'v']));
    $this->buildMeta();
  }
  public function getOne(){
    
    if(!empty($this->wheres)){
      return $this->quickLook();
    }else{
      return $this->readInRecord(1);      
    }
  }
  private function quickLook(){
    
    foreach($this->wheres as $where){
        if($where['attribute']=='id' && $where['operator']=='='){
          return $this->readInRecord($where['value']);
        }
    }
    return false;
    
  }
  public function getMany():array{
    $results =[];
    $keepReading = true;
    $this->validateLocation();
    $row = $this->getNextRecord();
    while($row && $keepReading){
      if($this->satisfiesWhere($row)){
        if($this->isNewGroup($row)){
          $results[] = $this->applySelects($row);
        }
      }   
      $keepReading = $this->keepReadingRecords($results);
      $row = $this->getNextRecord();
    }
    $results = $this->applyHaving($results);
    $results = $this->applyOrderBy($results);
    $results = $this->applyLimit($results);
    $this->clearState();
    return $results;
  }
  public function getCount():int{
    return 0;
  }
  public function update(array $data):bool{
    return true;
  }
  public function insert(array $data){
    return 1;
  }
  public function insertMany(array $data):bool{
    return true;
  }
  public function truncate():bool{
    return true;
  }
  public function create(array $cols):bool{
    return true;
  }
  public function destroy():bool{
    return true;
  }
  public function resourceExists():bool{
    return true;
  }
  
  public function clearState(){
    $this->cursor = -1;
  }
  protected function getNextRecord():array{
    $this->cursor++;
    if(isset($this->pkeys[$this->cursor])){
      $r = $this->readInRecord( $this->pkeys[$this->cursor] );
      return $r?$r:[];
    }else{
      return [];
    }
  }
  protected function validateLocation(){
    
  }
  private function keepReadingRecords(array $results):bool{
    return true;
  }
  private function applySelects(array $row):array{
    return $row;
  }
  private function applyHaving(array $results):array{
    return $results;
  }
  private function applyOrderBy(array $results):array{
    return $results;
  }
  private function applyLimit(array $results):array{
    return $results;
  }
  private function satisfiesWhere(array $row):bool{
    return true;
  }
  private function isNewGroup(array $row):bool{
    return true;
  }
  private function readInRecord($key){
    $d = $this->server->hget($this->getLocation(),$key);
    return json_decode($d,true);
  }
  private function readInAllRecords(){
    $d = $this->server->hgetAll($this->getLocation());
    foreach($d as $k=>$v){
      $d[$k] = json_decode($v,true);
    }
    return $d;
  }
  private function readInMeta(){
    $d = $this->server->hget($this->getLocation(),'meta');
    $meta = json_decode($d,true);
  }
  private function setMeta(array $meta){
    $this->pkeys = isset($meta['pkeys'])?$meta['pkeys']:[];
    
  }
  private function saveMeta($meta){
    $this->server->hset($this->getLocation(),'meta',json_encode($meta) );
    
  }
  private function buildMeta(){
    $records = $this->readInAllRecords();
    $meta['pkeys']=[];
    foreach($records as $key=>$row){
      if($key=='meta')continue;
      $meta['pkeys'][]= $row[$this->primary_key];
    }
    $this->setMeta($meta);
    $this->saveMeta($meta);
  }
}
/*
Redis index caching for quicker wheres
- no wheres
- no indexes in where
- mix indexable and non
- all indexable


Redis::
getMany()

getNext()
  if( haveIndexables() )
    if( firstCall() )
      buildIndexes()
    return getNextIndexedRow()
  else
    return getNextNondexRow()

buildIndexes()
  $pks = getAllPKs()
  foreach(wheres as w)
    $keys = getKeysForWhere(w)
    $pks = appearInBoth($pks,$keys)
    removeWhere(w)

getNextNondexRow()
  $k = getNextKey()
  return redis->get($k)

geNextKey()
  // pull in pk cache
  // pop next one and return
  


Abstract::
getMany()
  validateLocation()
  $results=[]
  
  while( $row = getNext() && $keepReading )
    if(satisfiesWhere($row) )
      if( isNewGroup($row) )
        $results[] = $row
    
    if( noOrdering() && limit = count($results) )
      $keepReading = false
        
   having()
   selects()
   orderBy()
   limit()
*/
