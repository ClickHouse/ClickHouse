<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
        xmlns:lxslt="http://xml.apache.org/xslt"
        xmlns:stringutils="xalan://org.apache.tools.ant.util.StringUtils">
<xsl:output method="html" indent="yes" encoding="UTF-8"
  doctype-public="-//W3C//DTD HTML 4.01 Transitional//EN" />
<xsl:decimal-format decimal-separator="." grouping-separator="," />
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
<xsl:template match="testsuites">
    <html>
        <head>
            <title>Test Results</title>
    <style type="text/css">

      body {
        font:normal 68% verdana,arial,helvetica;
        color:#000000;
      }
      table.details tr th{
        font-weight: bold;
        text-align:left;
        background:#ffcc00;
      }
      table.details tr td{
        background:#eeeeef;
      }
      p {
        line-height:1.5em;
        margin-top:0.5em; margin-bottom:1.0em;
      }
      h1 {
        margin: 0px 0px 5px; font: 165% verdana,arial,helvetica
      }
      h2 {
        margin-top: 1em; margin-bottom: 0.5em; font: bold 125% verdana,arial,helvetica
      }
      h3 {
        margin-bottom: 0.5em; color:#0077ff; font: bold 115% verdana,arial,helvetica
      }
      h4 {
        margin-bottom: 0.5em; font: bold 100% verdana,arial,helvetica
      }
      h5 {
        margin-bottom: 0.5em; font: bold 100% verdana,arial,helvetica
      }
      h6 {
        margin-bottom: 0.5em; font: bold 100% verdana,arial,helvetica
      }
      .Error {
        font-weight:bold; color:#ff3333;
      }
      .Failure {
        font-weight:bold; color:#0077ff;
      }
      .Properties {
        text-align:right;
      }
      </style>
       <!--
      <script type="text/javascript" language="JavaScript">
        var Projects = new Array();
        var cur;
        <xsl:for-each select="./testsuite">
            <xsl:apply-templates select="properties"/>
        </xsl:for-each>
       </script>
       <script type="text/javascript" language="JavaScript"><![CDATA[
        function displayProperties (name) {
          var win = window.open('','JUnitSystemProperties','scrollbars=1,resizable=1');
          var doc = win.document;
          doc.open();
          doc.write("<html><head><title>Properties of " + name + "</title>");
          doc.write("<style>")
          doc.write("body {font:normal 68% verdana,arial,helvetica; color:#000000; }");
          doc.write("table tr td, table tr th { font-size: 68%; }");
          doc.write("table.properties { border-collapse:collapse; border-left:solid 1 #cccccc; border-top:solid 1 #cccccc; padding:5px; }");
          doc.write("table.properties th { text-align:left; border-right:solid 1 #cccccc; border-bottom:solid 1 #cccccc; background-color:#eeeeee; }");
          doc.write("table.properties td { font:normal; text-align:left; border-right:solid 1 #cccccc; border-bottom:solid 1 #cccccc; background-color:#fffffff; }");
          doc.write("h3 { margin-bottom: 0.5em; font: bold 115% verdana,arial,helvetica }");
          doc.write("</style>");
          doc.write("</head><body>");
          doc.write("<h3>Properties of " + name + "</h3>");
          doc.write("<div align=\"right\"><a href=\"javascript:window.close();\">Close</a></div>");
          doc.write("<table class='properties'>");
          doc.write("<tr><th>Name</th><th>Value</th></tr>");
          for (prop in Projects[name]) {
            doc.write("<tr><th>" + prop + "</th><td>" + Projects[name][prop] + "</td></tr>");
          }
          doc.write("</table>");
          doc.write("</body></html>");
          doc.close();
          win.focus();
        }
      ]]>
      </script>
      -->
        </head>
        <body>
            <a name="top"></a>
            <xsl:call-template name="pageHeader"/>
            <!-- Summary part -->
            <xsl:call-template name="summary"/>
            <hr size="1" width="95%" align="left"/>
            <!-- For each class create the  part -->
            <xsl:call-template name="classes"/>
        </body>
    </html>
</xsl:template>
    <xsl:template name="classes">
        <!-- Сначала упавшие -->
        <xsl:for-each select="testsuite[count(testcase/failure) > 0]">
            <xsl:call-template name="testsuite"/>
        </xsl:for-each>
        <xsl:for-each select="testsuite[count(testcase/failure) = 0]">
            <xsl:call-template name="testsuite"/>
        </xsl:for-each>
    </xsl:template>
    <xsl:template name="testsuite">
      <!-- create an anchor to this class name -->
      <a href="#{@name}">
      <h3 id="{@name}"><xsl:value-of select="@name"/></h3>
      </a>
      <table class="details" border="0" cellpadding="5" cellspacing="2" width="95%">
        <xsl:call-template name="testcase.test.header"/>
        <!--
        test can even not be started at all (failure to load the class)
        so report the error directly
        -->
          <xsl:if test="./error">
              <tr class="Error">
                  <td colspan="4"><xsl:apply-templates select="./error"/></td>
              </tr>
          </xsl:if>
          <xsl:apply-templates select="./testcase" mode="print.test"/>
      </table>
      <p/>
      <a href="#top">Back to top</a>
    </xsl:template>
    <xsl:template name="summary">
        <h2>Summary</h2>
        <xsl:variable name="testCount" select="sum(testsuite/@tests)"/>
        <xsl:variable name="errorCount" select="sum(testsuite/@errors)"/>
        <xsl:variable name="failureCount" select="sum(testsuite/@failures)"/>
        <xsl:variable name="timeCount" select="sum(testsuite/@time)"/>
        <xsl:variable name="successRate" select="($testCount - $failureCount - $errorCount) div $testCount"/>
        <table class="details" border="0" cellpadding="5" cellspacing="2" width="95%">
        <tr valign="top">
            <th>Tests</th>
            <th>Failures</th>
            <th>Errors</th>
            <th>Success rate</th>
            <th>Time</th>
        </tr>
        <tr valign="top">
            <xsl:attribute name="class">
                <xsl:choose>
                    <xsl:when test="$failureCount &gt; 0">Failure</xsl:when>
                    <xsl:when test="$errorCount &gt; 0">Error</xsl:when>
                </xsl:choose>
            </xsl:attribute>
            <td><xsl:value-of select="$testCount"/></td>
            <td><xsl:value-of select="$failureCount"/></td>
            <td><xsl:value-of select="$errorCount"/></td>
            <td>
                <xsl:call-template name="display-percent">
                    <xsl:with-param name="value" select="$successRate"/>
                </xsl:call-template>
            </td>
            <td>
                <xsl:call-template name="display-time">
                    <xsl:with-param name="value" select="$timeCount"/>
                </xsl:call-template>
            </td>
        </tr>
        </table>
        <table border="0" width="95%">
        <tr>
        <td style="text-align: justify;">
        Note: <i>failures</i> are anticipated and checked for with assertions while <i>errors</i> are unanticipated.
        </td>
        </tr>
        </table>
    </xsl:template>
  <!--
   Write properties into a JavaScript data structure.
   This is based on the original idea by Erik Hatcher (ehatcher@apache.org)
   -->
  <!--
  <xsl:template match="properties">
    cur = Projects['<xsl:value-of select="../@package"/>.<xsl:value-of select="../@name"/>'] = new Array();
    <xsl:for-each select="property">
    <xsl:sort select="@name"/>
        cur['<xsl:value-of select="@name"/>'] = '<xsl:call-template name="JS-escape"><xsl:with-param name="string" select="@value"/></xsl:call-template>';
    </xsl:for-each>
  </xsl:template>
  -->
<!-- Page HEADER -->
<xsl:template name="pageHeader">
    <h1>Test Results</h1>
    <hr size="1"/>
</xsl:template>
<xsl:template match="testsuite" mode="header">
    <tr valign="top">
        <th width="80%">Name</th>
        <th>Tests</th>
        <th>Errors</th>
        <th>Failures</th>
        <th nowrap="nowrap">Time(s)</th>
    </tr>
</xsl:template>
<!-- class header -->
<xsl:template name="testsuite.test.header">
    <tr valign="top">
        <th width="80%">Name</th>
        <th>Tests</th>
        <th>Errors</th>
        <th>Failures</th>
        <th nowrap="nowrap">Time(s)</th>
        <th nowrap="nowrap">Time Stamp</th>
        <th>Host</th>
    </tr>
</xsl:template>
<!-- method header -->
<xsl:template name="testcase.test.header">
    <tr valign="top">
        <th>Name</th>
        <th>Status</th>
        <th width="80%">Type</th>
        <th nowrap="nowrap">Time(s)</th>
    </tr>
</xsl:template>
<!-- class information -->
<xsl:template match="testsuite" mode="print.test">
    <tr valign="top">
        <!-- set a nice color depending if there is an error/failure -->
        <xsl:attribute name="class">
            <xsl:choose>
                <xsl:when test="failures/text()[.&gt; 0]">Failure</xsl:when>
                <xsl:when test="errors/text()[.&gt; 0]">Error</xsl:when>
            </xsl:choose>
        </xsl:attribute>
        <!-- print testsuite information -->
        <td><a href="#{@name}"><xsl:value-of select="@name"/></a></td>
        <td><xsl:value-of select="tests/text()"/></td>
        <td><xsl:value-of select="errors/text()"/></td>
        <td><xsl:value-of select="failures/text()"/></td>
        <td>
            <xsl:call-template name="display-time">
                <xsl:with-param name="value" select="time/text()"/>
            </xsl:call-template>
        </td>
        <td><xsl:apply-templates select="@timestamp"/></td>
        <td><xsl:apply-templates select="@hostname"/></td>
    </tr>
</xsl:template>
<xsl:template match="testcase" mode="print.test">
    <tr valign="top">
        <xsl:attribute name="class">
            <xsl:choose>
                <xsl:when test="error">Error</xsl:when>
                <xsl:when test="failure">Failure</xsl:when>
                <xsl:otherwise>TableRowColor</xsl:otherwise>
            </xsl:choose>
        </xsl:attribute>
        <td><xsl:value-of select="@name"/></td>
        <xsl:choose>
            <xsl:when test="failure">
                <td>Failure</td>
                <td><xsl:apply-templates select="failure"/></td>
            </xsl:when>
            <xsl:when test="error">
                <td>Error</td>
                <td><xsl:apply-templates select="error"/></td>
            </xsl:when>
            <xsl:otherwise>
                <td>Success</td>
                <td></td>
            </xsl:otherwise>
        </xsl:choose>
        <td>
            <xsl:call-template name="display-time">
                <xsl:with-param name="value" select="@time"/>
            </xsl:call-template>
        </td>
    </tr>
</xsl:template>
<xsl:template match="failure">
    <xsl:call-template name="display-failures"/>
    <br/><br/>
    <code>
        <xsl:call-template name="br-replace">
            <xsl:with-param name="word" select="."/>
        </xsl:call-template>
    </code>
</xsl:template>
<xsl:template match="error">
    <xsl:call-template name="display-failures"/>
    <!-- display the stacktrace -->
    <br/><br/>
    <code>
        <xsl:call-template name="br-replace">
            <xsl:with-param name="word" select="."/>
        </xsl:call-template>
    </code>
</xsl:template>
<!-- Style for the error and failure in the tescase template -->
<xsl:template name="display-failures">
    <xsl:choose>
        <xsl:when test="not(@message)">N/A</xsl:when>
        <xsl:otherwise>
            <xsl:value-of select="@message"/>
        </xsl:otherwise>
    </xsl:choose>
    <xsl:choose>
        <xsl:when test="@linenumber">
            <br></br>
            at line <xsl:value-of select="@linenumber"/>
            <xsl:choose>
                <xsl:when test="@columnnumber">
                    , column <xsl:value-of select="@columnnumber"/>
                </xsl:when>
            </xsl:choose>
        </xsl:when>
    </xsl:choose>
</xsl:template>
<xsl:template name="JS-escape">
    <xsl:param name="string"/>
    <xsl:param name="tmp1" select="stringutils:replace(string($string),'\','\\')"/>
    <xsl:param name="tmp2" select="stringutils:replace(string($tmp1),&quot;'&quot;,&quot;\&apos;&quot;)"/>
    <xsl:value-of select="$tmp2"/>
</xsl:template>
<!--
    template that will convert a carriage return into a br tag
    @param word the text from which to convert CR to BR tag
-->
<xsl:template name="br-replace">
    <xsl:param name="word"/>
    <xsl:param name="splitlimit">32</xsl:param>
    <xsl:variable name="secondhalflen" select="(string-length($word)+(string-length($word) mod 2)) div 2"/>
    <xsl:variable name="secondhalfword" select="substring($word, $secondhalflen)"/>
    <!-- When word is very big, a recursive replace is very heap/stack expensive, so subdivide on line break after middle of string -->
    <xsl:choose>
      <xsl:when test="(string-length($word) > $splitlimit) and (contains($secondhalfword, '&#xa;'))">
        <xsl:variable name="secondhalfend" select="substring-after($secondhalfword, '&#xa;')"/>
        <xsl:variable name="firsthalflen" select="string-length($word) - $secondhalflen"/>
        <xsl:variable name="firsthalfword" select="substring($word, 1, $firsthalflen)"/>
        <xsl:variable name="firsthalfend" select="substring-before($secondhalfword, '&#xa;')"/>
        <xsl:call-template name="br-replace">
            <xsl:with-param name="word" select="concat($firsthalfword,$firsthalfend)"/>
        </xsl:call-template>
        <br/>
        <xsl:call-template name="br-replace">
            <xsl:with-param name="word" select="$secondhalfend"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="contains($word, '&#xa;')">
        <xsl:value-of select="substring-before($word, '&#xa;')"/>
        <br/>
        <xsl:call-template name="br-replace">
          <xsl:with-param name="word" select="substring-after($word, '&#xa;')"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
	<xsl:value-of select="$word"/>
      </xsl:otherwise>
    </xsl:choose>
</xsl:template>
<xsl:template name="display-time">
    <xsl:param name="value"/>
    <xsl:value-of select="format-number($value,'0.000')"/>
</xsl:template>
<xsl:template name="display-percent">
    <xsl:param name="value"/>
    <xsl:value-of select="format-number($value,'0.00%')"/>
</xsl:template>
</xsl:stylesheet>
