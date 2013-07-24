/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
 */

function DrawTotalViewsTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        document.getElementById('totalviews').innerHTML = data;
      }
    }
    connect.open('GET',  "TotalViews.php", true);
    connect.send(null);
  } catch(e) {
  }
}
