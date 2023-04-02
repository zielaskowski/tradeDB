function cpt_a() {
    var a;
    var b = cpt_t[0].value.toLowerCase();
    if (b.length == 4 && b != cpt_t_) {
        cpt_t_ = b;
        if (window.XMLHttpRequest) { a = new XMLHttpRequest(); }
        else if (window.ActiveXObject) {
            try { a = new ActiveXObject("Msxml2.XMLHTTP"); }
            catch (e) { try { a = new ActiveXObject("Microsoft.XMLHTTP"); } catch (e) { } }
        } a.onreadystatechange = function () { 
            if (a.readyState == 4 && a.status == 200) cpt_r(a.responseText); };

        a.open('GET', '//stooq.com/q/l/s/?t=' + b, true);
        a.setRequestHeader('Content-type', 'application/x-www-form-urlencoded');
        a.send(null);
    }

    else { cpt_r(2) }
    return false;
}