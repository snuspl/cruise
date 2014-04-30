##4월 11일 Meeting Note

###TODO
* StatePassing Tutorial revise & share via Github

### 연구 문의
* 해외 기업이나 학교와 공동연구 소개
	* Topic은 제안해 주시면 감사하겠습니다.
	* 다른 팀에서 선점할 수 있으니 빠르면 빠를수록 좋습니다.

### MS와 discuss한 내용
* Workload에 따라 elastic하게 동작하는 것 ; Policy의 문제
* 이기종 RM - UCLA에서 관심이 있음
* REEF MR
	* Shuffle 관련한 부분이 약함. 현재는 HDFS를 사용중, 성능에는 문제가 없음.
* Grouper
	* UW학생과 협업 - man power 부족
	* 공개는 아직
* ML도 작업중
* DAG Scheduler
	* 화요일 오전 미팅
	* Scheduling policy build
* Driver High Availability
	* MS에서도 requirement로 되어 있음
	* 누군가 작업을 할 예정
	* ZooKeeper or Corfu
* 특정 환경에서 REEF가 service 지원
* 학교 내에서 REEF 사용
	* 여름에 시작
* 현재 REEF 주안점
	* Core만 따로 분리 - WakeContrib / Reef-application
	* Code quality 향상
	* C# complete
	
### REEF Tutorial
* TANG, Wake, REEF의 Component와 사용 방법에 대해 tutorial


## 4월 4일 Meeting Note


### 다음 일정 (4월11일)
* Tutorial : 오전 10시반 정도
* 저녁 식사 : 저녁 6시 T-Tower

### 오전에 논의한 내용
주로 spec에 대한 구상을 많이 했습니다. 개발의 주안점을 어디에 둘 것인지, interface 정의를 어떻게 시작하면 좋을 지에 대해서 질문을 드렸습니다.

* File이 cache에 올려지는 과정은 Tachyon과 유사하게 구현합니다.
* DFS Client는 이미 많은 프로젝트에서 개발되었으므로 가져다 조금만 수정하면 될 것 같습니다.
* REEF를 이용하여 전체적인 상황을 monitoring 하도록 설계합니다. 원래 file이 어디에 있는지 monitor 해야하고, Fault-tolerance를 제공하면 좋겠습니다.
* Hadoop2.3.0에서 HDFS cache가 구현되었고, 중앙집중관리 관련 issue가 있습니다.

### 오후에 논의한 내용

* 5월말에 있는 due는 크게 신경쓰지 않으셔도 될 것 같습니다. 원래 계획대로 7월말까지 개발 완료하면 될 것 같습니다.
* Fault-tolerance를 위한 Data의 복제 및 분산 저장하는 것은 일단은 구현하지 않아도 괜찮을 것 같습니다.
* Broadcast policy : API의 형태로 제공하면 될 것 같습니다. 일단 이번 프로젝트에서는 용량이 작은 파일만 전 서버에 broadcast합니다. 
* _**Elasticity : Memory사용량에 따라 동적으로 자원을 할당합니다. Driver가 자원의 상황을 tracking하여 evaluator를 새로 할당하거나, 수거하여 유동적으로 동작하게 만듭니다.**_
* StorageService는 interface로 정의하여 차후 확장할 수 있도록 하고, 이 프로젝트에서는 HDFS에서 작동하는 service를 구현합니다.
* 각 Component의 역할
	* Application Client : Tachyon의 FSClient에 대응하는 class입니다.
	* NameServer : Tachyon의 master와 같은 역할을 수행합니다. reef-io의 network 패키지를 보면 Logical ID를 Socket Address과 연결짓는 NameServer가 구현되어 있습니다. Refactor하여 사용할 수 있을 것 같습니다.
	* Driver : Worker와 통신하여 자원 사용량을 monitor하고 유동적으로 Resource를 allocate합니다. 또한 heartbeat를 주고 받아 worker의 상태를 tracking 합니다. Elasticity/Resource Control담당. 필요시 Driver가 Evaluator 를 띄움
	* Inmemory Client : Driver를 통해서 Cache data에 access합니다.
	* Worker : Data를 HDFS에서 cache로 읽어 오거나, cache로부터 data를 읽는 것을 Context 에 Service로 등록하고, Task에서는 요청된 작업을 수행합니다. Broadcast는 examples에 있는 groupcomm/MatMultREEF 예제를 참조하여 구현하면 될 것 같습니다.

* 통신 protocol은 두 가지 방법으로 구현이 가능합니다.
	* Network Service : reef-io/network에 구현되어 있고, Logical ID를 사용할 수 있습니다.
	* Wake/RemoteManager : Event-driven 방식이라 더 편리할 것 같습니다. 새로운 worker가 추가되면 Driver가 Evaluator를 띄우고 reply를 받아 LogicalID 없이도 PhysicalAddress로 socket을 열 수 있습니다.
	
### 그 외
* Hadoop2.3.0을 사용하는 CDH 5가 release되어 이 환경에서 개발하게 됩니다. REEF가 2.3.0을 지원하면 좋을 것 같습니다..
* EC2 환경에서 실험해 볼만한 data 보내주시면 감사하겠습니다.
* REEF의 사용처, 향후 방향 등에 대해서 질문	
	* Computation 집중적인 기능은 개발중에 있습니다.
	* 하반기에는 DAG Scheduling을 통해 성능 향상을 할 수 있을까요
	* Workflow scheduler, API annotation expression 등도 개발예정입니다.
	* Mesos도 고려하여 design 되긴 했지만 implement는 안 되었습니다.
	* 나중에는 Job의 성격에 따라 ResourceManager를 고를 수 있을 것 같습니다.
* Memory를 elastic하게 할당하는 issue에 관심있는 사람들이 많습니다.

## 3월 21일 Meeting Note

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
	* Yarn의 scheduling 관련 여러 issue가 많으므로 jira issues 공유
* Spark와 tajo를 같이 쓰면 자원을 점유 - 활용 능력이 떨어짐
	* Omega ; shared state 를 track하여 workload에 따라 job을 분배?
* 진행중인 연구들
	* M/R on REEF	
		* shuffle에서의 fault tolerance 구현이 어려움
		* sailfish와 유사하나 not efficient 
		* onDisk로 하는 것은 구현 중이나 shuffle code가 복잡. Reseach 진행중 
	* ML - MPI 비슷, Fault aware & elastic 하게
* Checkpoint는 API 형태로 제공

