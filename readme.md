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

## Report
De [report](./query/report.py) module bevat de `Report` class. De `Report` biedt een workflow om van een [*markdown*](https://daringfireball.net/projects/markdown/) document naar een opgemaakt *html* rapport. Het resultaat kan eventueel ook geëxporteerd worden naar *docx*.
