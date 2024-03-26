--SELECT * FROM Sailors, Reserves, Boats Where Boats.D > 106 and Sailors.A = Reserves.G;
--SELECT * FROM Sailors, Reserves, Boats;
--SELECT * FROM Sailors, Reserves, Boats;
SELECT * FROM Sailors, Boats, Reserves Where Sailors.A = Reserves.G;