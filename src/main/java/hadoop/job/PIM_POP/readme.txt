商品主数据-pop sku表 
1000000006:1500214615^1000000001:1500214617，
格式是：颜色:颜色值^尺码:尺码值
但是^两端不固定哪个是颜色，哪个是尺码，需要根据WARE_CATEGORY_ATTRIBUTE.features来判断是否是"color:1"或"size:1"
1500214615通过VENDER_CATEGORY_VALUE.attribute_value来确定具体值 