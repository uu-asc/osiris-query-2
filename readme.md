# OSIRIS query 2
---
*OSIRIS query 2* is een python library die verschillende tools bevat om queries te maken en uit te voeren op een database. De library ondersteunt bij:

- het leggen van een verbinding met de database
- het uitvoeren van queries op de database
- het schrijven van queries
- het bewerken van resultaten
- het rapporteren over resultaten

## Queries uitvoeren
*OSIRIS query 2* abstraheert de stappen die nodig zijn voor het uitvoeren van een query. In andere woorden, de eindgebruiker hoeft zich in principe niet bezig te houden met de technische stappen die nodig zijn om een query uit te voeren. Hieronder is kort weergegeven welke modules achter de schermen verantwoordelijk zijn voor de uitvoer van een query.

### Connection
De `[connection](./query/connection.py)` module bevat koppeling naar verschillende databases. Een koppeling regelt de authenticatie en geeft een `Connection` terug die je vervolgens kunt gebruiken om queries uit te voeren.

### Execution
De [execution](./query/execution.py) module bevat de `execute_query` functie die uit de database op basis van een `Connection` de resultaten ophaalt van een opgegeven query.

### Definition
De [definition](./query/definition.py) module bevat het mechanisme waarmee opgeslagen queries kunnen worden ingelezen.

### Abstractie
De [osiris](./query/osiris.py) module is een voorbeeld van hoe *OSIRIS query 2* het uitvoeren van een query vereenvoudigt. De *credentials* (inloggegvens voor de OSIRIS database) zijn hier al gekoppeld aan de `execute_query` functie. De gebruiker hoeft daarom alleen maar een geldige *sql* statement op te geven on de query uit te voeren.

## Queries opstellen
Zoals gezegd is [definition](./query/definition.py) module verantwoordelijk voor het inlezen van queries. Deze module helpt echter op de volgende manieren ook bij het opstellen van queries:

- de queries zijn volledig parametriseerbaar met behulp van [*jinja2*](https://jinja.palletsprojects.com/en/3.0.x/) templating (zie in het bijzonder de [template designer documentation](https://jinja.palletsprojects.com/en/3.0.x/templates/) voor de mogelijkheden die dit biedt.) Bij het uitvoeren van een query geef je de benodigde parameters op via [*keyword arguments*](https://docs.python.org/3/tutorial/controlflow.html#keyword-arguments). Zoals bijvoorbeeld:

```python
sql = """
select *
from inschrijfhistorie
where collegejaar = {{ collegejaar }}
"""
osiris.execute_query(sql, collegejaar=2023)
```
- daarnaast zijn queries ook bij uitvoer nog *ad hoc* aan te passen. De module maakt gebruikt van [*sqlparse*](https://sqlparse.readthedocs.io/en/latest/) om de query te ontleden en van *jinja2* om aan te passen zodat je de query met *keyword arguments* op verschillende manieren kunt aanpassen:

> **CTE**
> - `select`: welke kolommen je wilt selecteren
> - `where`: welke rijen je wilt selecteren
> - `random`: of je de rijen in een willekeurige volgorde wilt zetten
> - `order_by`: hoe je de data wilt sorteren
> - `n`: het aantal rijen dat je wilt ophalen
>
> **AGGREGATION**
> - `aggfunc`: welke aggregatie functie je wilt toepassen
> - `distinct`: of je alleen aggregeert op unieke rijen
> - `columns`: op welke kolommen je wilt groeperen
> - `values`: welke waarden je wilt aggregeren
> - `label_val`: welk kolomnaam je gebruikt voor het aggregaat
> - `keep_na`: of je ook rijen met lege waarden meeneemt
> - `label_na`: welk label je gebruikt om lege waarden aan te duiden
> - `totals`: of het resultaat ook totalen moet bevatten
> - `grouping_sets`: van welke groepen je subtotalen wilt zien
> - `label_totals`: welk label je gebruikt om(sub)totalen aan te duiden

## Xquery
De resultaten worden opgehaald als [*pandas*](http://pandas.pydata.org/pandas-docs/stable/index.html) `DataFrame`. Het kan nodig zijn om verdere queries uit te voeren op de data in deze vorm. *pandas* biedt uitgebreide mogelijkheden om de data verder te bewerken. De [xquery]('./query/xquery) module voegt een methode toe aan de `DataFrame` om die verschillende functionaliteiten op een makkelijke manier te ontsluiten.

Er zijn drie veelgebruikte manieren om een `DataFrame` te filteren:
1. via een zogenaamde *boolean index*; een lijst van `True` en `False` waarbij de rijen die in de lijst de waarde `False` krijgen uit het resultaat worden verwijderd.
2. via een *functie*: de functie evalueert een rij uit de `DataFrame` en geeft `True` of `False` terug. Evenals bij de *boolean index* worden de rijen die `False` krijgen uit het resultaat verwijderd.
3. via een *query statement*: vergelijkbaar aan een *sql* statement maar gebaseerd op *python* (zie voor de syntax [deze pagina](http://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html#pandas.DataFrame.query)).

Met `xquery` kun je:
- bovengenoemde query methodes door elkaar heen gebruiken
- meerdere queries in één keer uitvoeren
- query resultaten verfijnen of query resultaten samenvoegen
- diverse statistieken bekijken over de uitgevoerde queries

Daarnaast past `xquery` ook de *query statement* aan. Je kunt een reguliere query schrijven maar je kunt ook een "eenvoudigere" syntax gebruiken die door `xquery` vervolgens vertaald wordt naar de *pandas* syntax. Verschillen met de *pandas* syntax zijn:

> **Multiline string**  
> Een query mag meerdere rijen beslaan:
> ```
> collegejaar = 2023
> and intrekking_vooraanmelding is null
>```
>
> **Equality operator**  
> In *python* wordt `==` gebruikt als *equality operator* (om aan te geven dat de waarde van `x` gelijk is aan de waarde van `y`). Een enkele `=` verwijst namelijk naar een *assignment* (het toewijzen van een waarde aan een bepaalde variabele). In *sql* wordt echter een enkele `=` gebruikt als *equality operator*. Omdat het in een *query statement* nooit zinvol is om een *assignment* te doen, interpreteert `xquery` de `=` als een *equality operator*.
>
> **Phrases**  
> Hoewel de *pandas* syntax het schrijven van een *query statement* een stuk makkelijker en leesbaarder maakt, zijn er diverse functionaliteiten die niet op een intuïtieve manier ontsloten worden. In `xquery` is een poging gedaan om dit verbeteren voor de volgende phrasen:
>
> | input                   | output                  |
> |-------------------------|-------------------------|
> | 'is na/null'            | `.isna()`               |
> | 'is not na/null'        | `.notna()`              |
> | "contains 'x'"          | `.str.contains('x')`    |
> | "matches 'x'"           | `.str.match('x')`       |
> | "full matches 'x'"      | `.str.fullmatch('x')`   |
> | "starts with 'x'"       | `.str.startswith('x')`  |
> | "ends with 'x'"         | `.str.endswith('x')`    |
> | "is alphanumeric"       | `.str.isalnum()`        |
> | "is alphabetic"         | `.str.isalpha()`        |
> | "is numeric"            | `.str.isnumeric()`      |
> | "is digit"              | `.str.isdigit()`        |
> | "is decimal"            | `.str.isdecimal()`      |
> | "is lowercase"          | `.str.islower()`        |
> | "is uppercase"          | `.str.isupper()`        |
> | "is titlecase"          | `.str.istitle()`        |
> | "is space"              | `.str.isspace()`        |
> | "is duplicated"         | `.duplicated(False)'`   |
> | "is first duplicated"   | `.duplicated('first')`  |
> | "is last duplicated"    | `.duplicated('last')`   |
> | 'xxxx-xx-xx'            | "'xxxx-xx-xx'"          |

## Pivot
De [pivot](./query/pivot.py) module bevat enkele functies die helpen bij het aggregaren van data in de `DataFrame` of `Series`. 

## Report
De [report](./query/report.py) module bevat de `Report` class. De `Report` biedt een workflow om van een [*markdown*](https://daringfireball.net/projects/markdown/) document naar een opgemaakt *html* rapport. Het resultaat kan eventueel ook geëxporteerd worden naar *docx*.
