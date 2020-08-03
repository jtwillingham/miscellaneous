drop table if exists default.jwillingham_dma_xwalk;

create external table default.jwillingham_dma_xwalk
(
  `nielsen_dma_code` int,
  `nielsen_dma_name` string,
  `acxiom_dma_code` int,
  `acxiom_dma_name` string,
  `inscape_dma_name` string
)
row format delimited fields terminated by '|'
location 's3://dxinnovation-useast1-dev/tv/panel_development/data/dma_xwalk/' tblproperties ('skip.header.line.count'='1');