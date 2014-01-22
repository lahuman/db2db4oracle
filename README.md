db2db4oracle
============
Hello Everyone~

This program is simple and easy copy to DB.


You will set information.

DBCopy.java

=================SOURCE 40 LINE==================
                
                //original information
		String ourl = "jdbc:oracle:thin:@IP:PORT/SID";
		String oid = "ID";
		String opw = "PW";
		
		//target information
		String turl = "jdbc:oracle:thin:@IP:PORT/SID";
		String tid = "ID";
		String tpw = "PW";
		
		//table key workd
		String tableLikeKeyword = "%Table_Name%";
		
		//ROW COUNT
		int maxRownum = 30000;
		
=================SOURCE 55INE==================

**NoTICE : Target table will be drop and create.

Good LUCK!

---------------------------------------------------
이 프로그램은 간단하게 DB를 카피 한는 프로그램 입니다.

다음 정보를 입력 하시고 실행하시면 됩니다.
DBCopy.java
=================SOURCE 40 LINE==================

                //원본 접속정보
		String ourl = "jdbc:oracle:thin:@IP:PORT/SID";
		String oid = "ID";
		String opw = "PW";
		
		//대상 접속정보
		String turl = "jdbc:oracle:thin:@IP:PORT/SID";
		String tid = "ID";
		String tpw = "PW";
		
		//테이블 키워드
		String tableLikeKeyword = "%Table_Name%";
		
		//최대 복사 행 수
		int maxRownum = 30000;
		
=================SOURCE 55INE==================

**공지 : 타겟 테이블을 drop 후 create 진행 합니다.

행운을 빌어요.
