<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:template match="mapping">
		<datalogCode>
			<xsl:for-each select="head/atom">
				<xsl:variable name="headAtom">
					<xsl:value-of select="@type" />
					<xsl:text>_</xsl:text>
					<xsl:value-of select="substring-before(atomValue, '(')" />
				</xsl:variable>
				<xsl:choose>
					<xsl:when test="@negated = 'true'">
						<xsl:text>NOT(</xsl:text>
						<xsl:value-of select="$headAtom" />
						<xsl:text>)</xsl:text>
					</xsl:when>
					<xsl:otherwise>
						<xsl:value-of select="$headAtom" />
					</xsl:otherwise>
				</xsl:choose>
				<xsl:if test="position() &lt;= last()-1">
					<xsl:text>, </xsl:text>
				</xsl:if>
				<xsl:if test="position() = last()">
					<xsl:text> :- </xsl:text>
				</xsl:if>
			</xsl:for-each>
			<xsl:for-each select="body/atom">
				<xsl:variable name="bodyAtom">
					<xsl:value-of select="@type" />
					<xsl:text>_</xsl:text>
					<xsl:value-of select="substring-before(atomValue, '(')" />
				</xsl:variable>
				<xsl:choose>
					<xsl:when test="@negated = 'true'">
						<xsl:text>NOT(</xsl:text>
						<xsl:value-of select="$bodyAtom" />
						<xsl:text>)</xsl:text>
					</xsl:when>
					<xsl:otherwise>
						<xsl:value-of select="$bodyAtom" />
					</xsl:otherwise>
				</xsl:choose>
				<xsl:if test="position() &lt;= last()-1">
					<xsl:text>, </xsl:text>
				</xsl:if>
			</xsl:for-each>
		</datalogCode>
	</xsl:template>
	<!-- standard copy template -->
	<xsl:template match="@*|node()">
		<xsl:copy>
			<xsl:apply-templates select="@*"/>
			<xsl:apply-templates/>
		</xsl:copy>
	</xsl:template>	
</xsl:stylesheet>