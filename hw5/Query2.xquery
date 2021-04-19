for $pid in distinct-values(doc("xmls/purchaseorders.xml")/PurchaseOrders/PurchaseOrder/item/partid)
    let $price := doc("xmls/products.xml")/products/product[@pid = $pid]/description/price
    let $item := doc("xmls/purchaseorders.xml")/PurchaseOrders/PurchaseOrder/item[partid = $pid]
    
order by $pid 
return <totalcost partid = "{$pid}"> "{sum($item/quantity) * $price}" </totalcost>