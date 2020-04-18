Was ist drin in den Daten einer einzelnen Route? Ich habe mir das mal anhand der Route 31414_3 veranschaulicht:

Eine Route kann eine Menge an trips (einige hunderte) enthalten, von denen jeder höchstens einmal pro Tag stattfindet.

Je Kombination aus Datum (nur indirekt in den Timestamps enthalten) und trip_id gibt es nun einige tausende Datenpunkte. Die bestehen aus Prognosen für künftige Halte, wobei in jedem trip_update eine Reihe von künftigen stop_times enthalten sein kann. Für jede stop_id entstehen während der Annäherung an den Haltepunkt eine Reihe von Prognosen, wobei die letzte Prognose des jeweiligen Tages als die "echte" Abfahrtszeit angesehen werden kann / muss.

Hier können wir entscheiden, ob wir nur mit diesen "definitiven" Werten arbeiten wollen, oder die Veränderung der Anbieter-Prognosen analysieren wollen. Eine erste Stichprobe hat gezeigt, dass 80% der Prognosen konstant bleiben, obwohl diese oft mehr als 20 mal "aktualisiert" werden.

## Idee:

Zu jeder Stop-ID die vorgänger und nachfolger in allen trips sammeln. Dabei für trips der Rückrichtung entsprechend vorgänger und nachfolger tauschen. Streng genommen reichen auch nur Nachfolger.

Dann einen Sortieralgorithmus drüber laufen lassen, mit dem Vergleichskriterium: ist a Nachfolger von b, oder b Nachfolger von a, oder nichts von beiden?

Dabei könnten unter gewissen umständen Zyklen auftreten, dann könnte keine Sortierung gefunden werden.

Idee:
Für zwei sequenzen von stop_ids lässt sich feststellen, ob diese gleiche oder verschiedene Richtugng haben, oder sich nicht überlappen, oder sich ihrere Richtungen gar widersprechen.

Ausgehend von einer willkürzlich gewählten startsequenz, deren Richtung als "0" definiert wird, kann jede weitere also auch mit "0" oder "1" markiert werden, oder als inkompatibel aus dem Pool genommen werden, oder als noch nicht zu klären zurück gestellt werden.

## Grafische Darstellung
Routen enthalten oft mehrere stop_ids mit gleichem Name. Für die Darstellung sollten diese als identisch angesehen werden.


Ein Bildfahrplan je shape_id. Diverse trips haben diese shape_id, vermutlich fast immer trips der selben route. 


## TODO Sonntag
Im Moment habe ich noch eine wilde Menge von Datenpunkten.

Die müsste ich nun nach Tag und trip_id gruppieren, und innerhalb jeder solchen Kombination je stop_id den jeweils neusten Eintrag wählen. Dann noch die verbleibenden Punkte nach stop_sequence sortieren und als Linienzug darstellen.

## TODO Samstag
Finde unter allen shapes / route variants eine mit maximaler länge, und dann alle, die dort vorwärts oder rücktwärts hinein passen. Unter allen verbliebenen, gehe wieder genauso vor. Im Grunde genommen partitioniert das die Liste der verbleibenden Shapes in jene, die passen, und jene, die immernoch verbleiben.

Dann Je Master-Shape eine Grafik, die genau jene Sub-Shapes enthält, die dem Master zugeordnet wurden. Die Rückwärts-Darstellung müsste sich von ganz allein ergeben, d.h. wir müssten nicht mitschreiben, welche Shapes wir umkehren mussten.

Außerdem: Clientseitig bei der route-Suche nach source filtern.

## Wie das derzeit arbeitet:

 * route-ids -> …for_route -> …for_route_variants -> …for_trips
 * shape-ids -> …for_shapes -> …for_trips
 * all -> …for_route  -> …for_route_variants -> …for_trips

