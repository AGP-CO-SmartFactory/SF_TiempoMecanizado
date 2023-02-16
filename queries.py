df_zferlist_query = '''WITH A as (Select OP.Ordem_Serial, OP.Ordem_CodMaterial, 
	CASE WHEN D.Desenho_Descricao LIKE '%BORO%' THEN 1 ELSE 0 END AS ALUMINUM, 
	CASE WHEN D.Desenho_Descricao LIKE 'MAAC%' THEN 1 ELSE 0 END AS ACERO, 
	CASE WHEN D.Desenho_Descricao LIKE '%TNT%' THEN 1 ELSE 0 END AS MALLA, 
	CASE WHEN D.Desenho_Descricao LIKE 'CPET%' THEN 1 ELSE 0 END AS AL, 
	CASE WHEN D.Desenho_Descricao LIKE 'PVTE%' THEN 1 ELSE 0 END AS TEJIDO, D.Desenho_Descricao 
	FROM SAGA_OrdensProducao OP 
		inner join SAGA_Desenhos D on D.Desenho_OrdemSerial = OP.Ordem_Serial
		WHERE Ordem_Centro in ('CO01') 
		and (Desenho_Descricao like '%BORO%' OR Desenho_Descricao like 'MAAC%' OR Desenho_Descricao like '%TNT%' or Desenho_Descricao like 'CPET%' or Desenho_Descricao like 'PVTE%')),
		
Materiales as (Select Ordem_Serial, CAST (Ordem_CodMaterial as int) as Ordem_CodMaterial, 
SUM(ALUMINUM) as Aluminum, SUM(ACERO) as Acero, SUM(MALLA) as Malla, SUM(AL) as AL, SUM(TEJIDO) as Tejido 
From A 
Group by Ordem_Serial, Ordem_CodMaterial)

SELECT O.Ordem_CodMaterial as ZFER, Desenho_ClaveModelo as CLV_MODEL, Desenho_Name, Desenho_ZTipo, CAST(Desenho_CodMat as int) as CodMat, D.ADICIONAL_1 as POS,
	CAST(D.ADICIONAL_2 as float) as AreaConsumo, ISNULL(M.Acero, 0) as Acero, ISNULL(M.AL, 0) as Al, 
	ISNULL(M.Aluminum, 0) as Boro, ISNULL(M.Malla, 0) as TNT, ISNULL(M.Tejido, 0) as PVTE
FROM SAGA_Desenhos D
	inner join SAGA_OrdensProducao O on D.Desenho_OrdemSerial = O.Ordem_Serial
	left join Materiales M on D.Desenho_OrdemSerial = M.Ordem_Serial
WHERE Desenho_OrdemSerial like '1%'
AND O.Ordem_Centro = 'CO01'
AND Datalength(Desenho_Name) > 0
AND Desenho_ZTipo like 'Z_%'
GROUP BY O.Ordem_CodMaterial, Desenho_ClaveModelo, Desenho_Name, Desenho_ZTipo, Desenho_CodMat, D.ADICIONAL_5, D.ADICIONAL_1, D.ADICIONAL_2, M.Acero, M.AL, M.Aluminum, M.Malla, M.Tejido
ORDER BY ZFER, CLV_MODEL'''

