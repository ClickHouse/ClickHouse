-- Functions `extractHTMLOne` and `extractHTMLAll`: CSS selectors over HTML.

SELECT '-- basic';
SELECT extractHTMLOne('<div><p>Hello</p><p>World</p></div>', 'p');
SELECT extractHTMLOne('<div><p>Hello</p></div>', 'span') = '';
SELECT extractHTMLAll('<div><p>Hello</p><p>World</p></div>', 'p');
SELECT extractHTMLAll('<div><p>Hello</p></div>', 'span');
SELECT extractHTMLAll('<UL><LI>a</LI><Li>b</lI></UL>', 'ul > li');
SELECT extractHTMLAll('<div>a<div>b</div>c</div>', 'div');
SELECT extractHTMLAll('<p>1</p><p>2</p>', '*');

SELECT '-- id and class';
SELECT extractHTMLAll('<p id="a" class="x y">1</p><p class="xy">2</p><p class=" y  x ">3</p><p class="X">4</p>', '.x::text');
SELECT extractHTMLAll('<p id="a" class="x y">1</p><p class="xy">2</p><p class=" y  x ">3</p>', '.x.y#a::text');
SELECT extractHTMLAll('<p id="a">1</p><p id="A">2</p><p id="a b">3</p>', '#a::text');
SELECT extractHTMLOne('<p id="1a">x</p>', '#\\31 a');

SELECT '-- attributes';
SELECT extractHTMLAll('<a href=/u>1</a><a HREF="/v">2</a><a name=x>3</a><a href>4</a>', '[href]::text');
SELECT extractHTMLAll('<a lang="en">1</a><a lang="en-US">2</a><a lang="english">3</a><a lang="EN">4</a>', 'a[lang|=en]::text');
SELECT extractHTMLAll('<a lang="en">1</a><a lang="en-US">2</a><a lang="english">3</a><a lang="EN">4</a>', 'a[lang=en i]::text');
SELECT extractHTMLAll('<a href="https://a.com/x.pdf">1</a><a href="http://b.org/y.PDF">2</a><a href="/z.html">3</a>', 'a[href^=https]::text');
SELECT extractHTMLAll('<a href="https://a.com/x.pdf">1</a><a href="http://b.org/y.PDF">2</a><a href="/z.html">3</a>', 'a[href$=".pdf"]::text');
SELECT extractHTMLAll('<a href="https://a.com/x.pdf">1</a><a href="http://b.org/y.PDF">2</a><a href="/z.html">3</a>', 'a[href$=".pdf" i]::text');
SELECT extractHTMLAll('<a href="https://a.com/x.pdf">1</a><a href="http://b.org/y.PDF">2</a><a href="/z.html">3</a>', 'a[href*="b.org"]::text');
SELECT extractHTMLAll('<a rel="nofollow noopener">1</a><a rel="nofollowx">2</a>', 'a[rel~=nofollow]::text');
SELECT extractHTMLAll('<a title="">1</a><a>2</a>', 'a[title=""]::text');
SELECT extractHTMLAll('<a title="">1</a><a title="x">2</a>', 'a[title^=""]::text');
SELECT extractHTMLAll('<a title="a b">1</a>', 'a[title~="a b"]::text');
SELECT extractHTMLAll('<input type=text value=\'it"s\'><input type="checkbox" checked>', 'input[checked]::attr(type)');
SELECT extractHTMLAll('<input type=text value=\'it"s\'><input type="checkbox" checked>', 'input::attr(value)');
SELECT extractHTMLAll('<p a=1 a=2>x</p>', 'p::attr(a)');
SELECT extractHTMLOne('<a title="a > b" href="/x">link</a>', 'a');
SELECT extractHTMLOne('<a href=/x/>link</a>', 'a');

SELECT '-- case sensitivity of attribute values';
SELECT extractHTMLAll('<input type="TEXT" value="1"><input type="text" value="2"><input type="radio" value="3">', 'input[type=text]::attr(value)');
SELECT extractHTMLOne('<input type="TEXT" value="1">', 'input[type=text]::attr(value)');
SELECT extractHTMLAll('<input type="TEXT" value="1"><input type="text" value="2"><input type="radio" value="3">', 'input:not([type=text])::attr(value)');
SELECT extractHTMLAll('<input type="TEXT" value="1"><input type="text" value="2"><input type="radio" value="3">', 'input[type=text s]::attr(value)');
SELECT extractHTMLAll('<input type="TEXT" value="1"><input type="text" value="2"><input type="radio" value="3">', 'input[type="text"S]::attr(value)');
SELECT extractHTMLAll('<a rel="NOFOLLOW Noopener">1</a><a rel="nofollowx">2</a>', 'a[rel~=nofollow]::text');
SELECT extractHTMLAll('<form method="POST">1</form><form method="get">2</form>', 'form[method^=po]::text');
SELECT extractHTMLAll('<p title="Hello">1</p><p title="hello">2</p>', 'p[title=hello]::text');
SELECT extractHTMLAll('<p title="Hello">1</p><p title="hello">2</p>', 'p[title="hello"i]::text');
SELECT extractHTMLAll('<p id="A" class="X">1</p>', '#a, .x, [id=a], [class~=x]');

SELECT '-- combinators';
SELECT extractHTMLAll('<div><span>1</span><p><span>2</span></p></div><span>3</span>', 'div > span::text');
SELECT extractHTMLAll('<div><span>1</span><p><span>2</span></p></div><span>3</span>', 'div span::text');
SELECT extractHTMLAll('<div class="a"><div><div class="b"><span>1</span></div></div></div><div class="b"><span>2</span></div>', '.a .b span::text');
SELECT extractHTMLAll('<div class="a"><div><p><span>1</span></p></div><p><span>2</span></p></div>', '.a > p span::text');
SELECT extractHTMLAll('<h2>T</h2><p>1</p><p>2</p><h2>U</h2><div></div><p>3</p>', 'h2 + p::text');
SELECT extractHTMLAll('<h2>T</h2><p>1</p><p>2</p><h2>U</h2><div></div><p>3</p>', 'h2 ~ p::text');
SELECT extractHTMLAll('<h2>T</h2>text<!-- comment --><p>1</p>', 'h2+p::text');
SELECT extractHTMLAll('<div><h2>T</h2></div><p>1</p>', 'h2 ~ p::text');
SELECT extractHTMLAll('<ul><li><b>1</b></li><li><b>2</b><i>3</i></li></ul>', 'li + li > b ~ i::text');

SELECT '-- pseudo-classes';
SELECT extractHTMLAll('<ul><li>1<li>2<li>3<li>4<li>5</ul>', 'li:nth-child(odd)::text');
SELECT extractHTMLAll('<ul><li>1<li>2<li>3<li>4<li>5</ul>', 'li:nth-child(even)::text');
SELECT extractHTMLAll('<ul><li>1<li>2<li>3<li>4<li>5</ul>', 'li:nth-child(3n)::text');
SELECT extractHTMLAll('<ul><li>1<li>2<li>3<li>4<li>5</ul>', 'li:nth-child(-n + 2)::text');
SELECT extractHTMLAll('<ul><li>1<li>2<li>3<li>4<li>5</ul>', 'li:nth-child(2n+3)::text');
SELECT extractHTMLAll('<ul><li>1<li>2<li>3<li>4<li>5</ul>', 'li:nth-child(4)::text');
SELECT extractHTMLAll('<ul><li>1<li>2<li>3<li>4<li>5</ul>', 'li:first-child::text');
SELECT extractHTMLAll('<ul><li>1<li>2<li>3<li>4<li>5</ul>', 'li:not(:first-child, :nth-child(4))::text');
SELECT extractHTMLAll('<div><p class="x">1</p><p>2</p><span>3</span></div>', 'div > :not(p)::text');
SELECT extractHTMLAll('<div><p class="x">1</p><p>2</p><span>3</span></div>', 'p:not(.x)::text');
SELECT extractHTMLAll('text<p>1</p><div><p>2</p></div>', ':root::text');

SELECT '-- selector lists';
SELECT extractHTMLAll('<h1>a</h1><h2>b</h2><h3>c</h3>', 'h3, h1');
SELECT extractHTMLAll('<a href="/a">A</a><img src="/i.png"><a>B</a>', 'a::attr(href), img::attr(src)');
SELECT extractHTMLAll('<p class="x y">1</p>', '.x, .y, p');

SELECT '-- text';
SELECT extractHTMLOne('<div>Hello, <b>world</b>!<script>var x = 1;</script>\n  <!-- comment --> Bye.</div>', 'div::text');
SELECT extractHTMLOne('<head><title>\n  My   Title\n</title></head>', 'title::text');
SELECT extractHTMLOne('<script type="application/ld+json">\n{"@type": "Person", "name": "<b>Alice</b>"}\n</script>', 'script[type="application/ld+json"]::text');
SELECT extractHTMLOne('<textarea><b>bold</b>  text</textarea>', 'textarea::text');
SELECT extractHTMLAll('<p>a</p><p></p><br>', '*::text');

SELECT '-- raw text and comments';
SELECT extractHTMLAll('<script>if (a < b) { x = "<div>"; }</script><div>ok</div>', 'div');
SELECT extractHTMLAll('<style>p > a { color: red }</style><p>ok</p>', 'p');
SELECT extractHTMLAll('<SCRIPT>x = "</scripts>";</Script ><p>ok</p>', 'script');
SELECT extractHTMLAll('<!-- <p>no</p> --><p>yes</p><!--><p>yes2</p><!---->', 'p::text');
SELECT extractHTMLAll('<![CDATA[<p>no</p>]]><p>yes</p>', 'p::text');
SELECT extractHTMLAll('<!DOCTYPE html><?xml version="1.0"?><p>yes</p>', 'p::text');
SELECT extractHTMLAll('a < b <p>yes</p> c <> d </ e> f', 'p');

SELECT '-- void and self-closing elements';
SELECT extractHTMLAll('<p>a<br>b<img src=x.png>c</p>', 'img');
SELECT extractHTMLAll('<p>a<br>b<img src=x.png>c</p>', 'p');
SELECT extractHTMLAll('<p>a<br/>b<IMG src="x.png" />c</p>', 'p > *');
SELECT extractHTMLAll('<svg><circle r="1"/><circle r="2"></circle></svg>', 'svg > circle');

SELECT '-- implicitly closed elements';
SELECT extractHTMLAll('<p>one<p>two<div>three</div>', 'p');
SELECT extractHTMLAll('<ul><li>a<li>b<ul><li>c<li>d</ul><li>e</ul>', 'li');
SELECT extractHTMLAll('<ul><li>a<li>b<ul><li>c<li>d</ul><li>e</ul>', 'ul > li:nth-child(2)');
SELECT extractHTMLAll('<table><tr><td>1<td>2<tr><td>3<td>4</table>', 'tr');
SELECT extractHTMLAll('<table><tr><td>1<td>2<tr><td>3<td>4</table>', 'tr > td:nth-child(2)::text');
SELECT extractHTMLAll('<table><thead><tr><th>h<tbody><tr><td>1<tr><td>2</table>', 'tbody td::text');
SELECT extractHTMLAll('<table><tr><td><table><tr><td>inner</table><td>outer</table>', 'table > tr > td:nth-child(2)::text');
SELECT extractHTMLAll('<select><option>a<option selected>b<optgroup label=g><option>c</select>', 'option::text');
SELECT extractHTMLAll('<select><option>a<option selected>b<optgroup label=g><option>c</select>', 'optgroup > option::text');
SELECT extractHTMLAll('<dl><dt>a<dd>b<dt>c<dd>d</dl>', 'dt + dd::text');
SELECT extractHTMLAll('<a href=1>x<a href=2>y</a>', 'a');
SELECT extractHTMLAll('<html><head><title>T</title><body><p>x</p></body></html>', 'html > body > p');

SELECT '-- unmatched and missing end tags';
SELECT extractHTMLAll('<div><span>a</div><span>b</span>', 'div');
SELECT extractHTMLAll('<div><b>x</span>y</b></div>', 'b');
SELECT extractHTMLAll('<div><b>x</div>y</b>', 'b');
SELECT extractHTMLAll('<table><tr><td><div>x</td><td>y</td></table>', 'td');
SELECT extractHTMLAll('<div><p>a', 'p');
SELECT extractHTMLAll('<div><p>a', 'div');
SELECT extractHTMLAll('<div><p class="x', 'p');
SELECT extractHTMLAll('<div><p class="x"', 'div');
SELECT extractHTMLAll('<p>a</p', 'p');
SELECT length(extractHTMLAll(repeat('<div>', 1000), 'div'));
SELECT length(extractHTMLAll(repeat('<div>', 1000), 'div div'));

SELECT '-- columns';
SELECT extractHTMLOne(html, sel), extractHTMLAll(html, sel)
FROM VALUES('html String, sel String',
    ('<p>1</p><b>2</b><b>3</b>', 'p'), ('<p>1</p><b>2</b><b>3</b>', 'b'), ('<p>4</p><b>5</b><b>6</b>', 'b'), ('', 'b'), ('<i>7</i>', 'i::text'));
SELECT extractHTMLOne(materialize('<p>1</p><p>2</p>'), 'p:nth-child(2)'), extractHTMLAll(materialize('<p>1</p><p>2</p>'), 'p') FROM numbers(2);
SELECT extractHTMLOne('<p>1</p>', materialize('p::text')) FROM numbers(2);
SELECT extractHTMLOne(NULL, 'p'), extractHTMLAll(NULL, 'p'), extractHTMLOne(toNullable('<p>1</p>'), 'p');
SELECT extractHTMLOne(toLowCardinality('<p>1</p>'), 'p'), toTypeName(extractHTMLAll('', 'p'));

SELECT '-- errors';
SELECT extractHTMLOne('<p>', ''); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', '   '); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p:last-child'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p:has(a)'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p::before'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p::text a'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'div >'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'div >> p'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p,'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'svg|rect'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p[a'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p[a=]'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p[a="x]'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p[a!=x]'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p:nth-child(x)'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p:nth-child(2'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p:not(a b)'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', 'p:not(:not(a))'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', '.'); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', arrayStringConcat(arrayMap(x -> 'p', range(65)), ' ')); -- { serverError BAD_ARGUMENTS }
SELECT extractHTMLOne('<p>', arrayStringConcat(arrayMap(x -> 'p', range(64)), ' '));
SELECT extractHTMLOne('<p>'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT extractHTMLOne(1, 'p'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT extractHTMLAll('<p>', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
