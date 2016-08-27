$.fn.dataTable.ext.order['date-relative'] = function(settings, col)
{
    return this.api().column(col, {order:'index'} ).nodes().map(function(td, i) {
        return $(td).attr('title');
    });
}
