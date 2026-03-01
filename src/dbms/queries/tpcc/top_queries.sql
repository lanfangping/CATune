SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05,
	           S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10
	      FROM stock
	     WHERE S_I_ID = '56222'
	       AND S_W_ID = '2' FOR UPDATE
	
;
INSERT INTO order_line
	     (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)
	     VALUES ('3001','7','6','1','18147','6','6','139.13999938964844','czbehtysbjvhtgyubpyxbbe '),('3001','7','6','2','22439','6','4','254.24000549316406','gfoabtdglqnszturdezqrar '),('3001','7','6','3','47583','6','7','212.3800048828125','wnskjpaminrmwjyszpawvqm '),('3001','7','6','4','30243','6','9','852.75','ajfklmphfretmetngyleehj '),('3001','7','6','5','64707','6','8','444.7200012207031','pgkapbbkmjtbhjtpeoywwfc '),('3001','7','6','6','73203','6','5','402.20001220703125','ygpddbjpyynafxgnrhwapxr '),('3001','7','6','7','65255','6','2','89.12000274658203','pepjcshssqoednfvrorsusv '),('3001','7','6','8','81639','6','1','23.8700008392334','wpsfcimjiikwujtchbzvmxc ')
	
;
UPDATE stock
	       SET S_QUANTITY = '99' ,
	           S_YTD = S_YTD + '6',
	           S_ORDER_CNT = S_ORDER_CNT + 1,
	           S_REMOTE_CNT = S_REMOTE_CNT + '0'
	     WHERE S_I_ID = '18147'
	       AND S_W_ID = '6'
	
;
SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT
	     FROM  order_line, stock
	     WHERE OL_W_ID = '10'
	     AND OL_D_ID = '10'
	     AND OL_O_ID < '3001'
	     AND OL_O_ID >= '2981'
	     AND S_W_ID = '10'
	     AND S_I_ID = OL_I_ID
	     AND S_QUANTITY < '20'
	
;
SELECT I_PRICE, I_NAME , I_DATA
	      FROM item
	     WHERE I_ID = '13503'
	
;
