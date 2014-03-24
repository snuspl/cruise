## 3월 21일 Meeting Note
=====

### TODO
* Kickoff Meeting : 3/28(금) 5시
* 금요일 상주 공간 확보
* YARN setup
* REEF Full paper 공유 가능 여부 Discuss

### Milestone
Target이 분명한 InMemory Part는 7월까지 확실히 진행하도록 하고, Scheduling은 여러 방안을 고려해 보고 차후에 선택하도록 한다.

| 시간 | 내용 |
| --- | --- |
|  현재-5월  |  박원장님께 시연 / Interface 위주 |
|  7월  |  개발완료  |
|  8-9월  |  Test  |
|  9월 |  제품완료  |


### Environment
* 개발환경 - CDH5.0 version ( Java7 / Hadoop2.2.0 )
* Cluster 환경 - Machine 40대 / CPU 32 Core / Memory 368GB
* Table 구조 - ETL / DW / back-end MART / front-end MART
	1. back-end MART
		* 고급OLAP
		* 질의가능 Table - 7T/0.3T/100GB
		* LTE가 되면서 3배씩 증가된 양 포함
		* 13개월치 Data
		* Web에서 수행 후 완료시 notify
		* Tajo 사용 ; DataNode를 통해서 가져오는데 사용하게 될 지 확실치는 않음
		* M/R에서 하는 것과 동일했으면 좋겠다.
	* front-end MART
		* 정형report
		* 3TB total
		* 1 table < 128MB
		* 13 개월치
		* 큰 table 1, 나머지들은 압축 code값들이 join된 형태
		* size : 1G~2G scale
		* *모든 cluster에 distribute 되었으면*
		* Web에서 즉각적으로 response
		* Shark 사용 ; Ad-hoc queries 처리
##### Workload의 특성
* Data가 하룻동안은 변하지 않음
	
### Requirements
Spark or M/R를 InMemory에서 동작하도록 구현 - locality, loading, broadcasting

* 5월말까지는 Read from HDFS
	* Block size ; 100MB per table 이면 충분
	* 더 큰 size / Write to HDFS는 이후에 고려 
* 작은 Data를 모든 node에 replicate
	* 1 large table / Many small table 구조
* onDemand방식이 아니라, configuration interface 제공
	* e.g. "일정 크기 이상 모두 복제"

* URL로부터 cache 에 있는 내용 불러옴
	* NameNode와 같은 역할을 하는 Directory path가 필요
	* URL을 Hashing하여 <K, V> 형태로 저장도 가능
	
* Shark가 Main part. (Streaming은 이번 project의 범위가 아님)

* FSInputStream과의 호환성

* Spark와의 interface, 
	* YARN위에서 구동시 Performance 향상
	* AM 생성시 수반하는 latency 개선


##### 1차적 목표
* ELT M/R을  주기적으로 하는 것이 점유율이 80% ; too bad.
* Data가 계속적으로 들어오는 건 상관없음. 속도가 가장 큰 issue
* Web front-end에서 1GB / Ad-hoc에서 한달치 정도가 Memory에 올라갔으면
* 결과적으로 3개월 정도치가 빠르게 돌아갔으면 좋겠다.


### Other Issues
* HDFS Caching과의 비교우위
* Fault tolerance
	* REEF Driver는 계속 자원을 점유하지만 YARN이 죽으면 죽음
	* 일단은 ZooKeeper사용. Driver fail에 대한 대응은 차후에
* Scheduling 관련 이슈
	* Yarn의 scheduling 관련 여러 issue가 많으므로 Zira issues 공유
* Spark와 tazo를 같이 쓰면 자원을 점유 - 활용 능력이 떨어짐
	* Omega ; shared state 를 track하여 workload에 따라 job을 분배?
* 진행중인 연구들
	* M/R on REEF	
		* shuffle에서의 fault tolerance 구현이 어려움
		* sailfish와 유사하나 not efficient 
		* onDisk로 하는 것은 구현 중이나 shuffle code가 복잡. Reseach 진행중 
	* ML - MPI 비슷, Fault aware & elastic 하게
* Checkpoint는 API 형태로 제공

