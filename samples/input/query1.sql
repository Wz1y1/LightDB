--SELECT * FROM Sailors;
--SELECT * FROM Boats WHERE Boats.E = 2 AND Boats.F = 3;
--SELECT * FROM Boats;
--SELECT Sailors.A, Boats.D
--FROM Sailors
--JOIN Boats ON Sailors.A = Boats.F;
--SELECT * FROM Sailors, Boats;
--SELECT * FROM Sailors JOIN Reserves ON Sailors.C > Reserves.H;
--SELECT * FROM Sailors where Sailors.C > Reserves.H;

--SELECT * FROM Sailors, Reserves WHERE Sailors.C > Reserves.H and Sailors.C > 100;

--SELECT Sailors.A, Boats.F
--FROM Sailors
--WHERE Sailors.A = Boats.F;
--SELECT * FROM Sailors ORDER BY Sailors.B;
--SELECT * FROM Reserves, Sailors, Boats WHERE 102 = Boats.D AND Sailors.B = 200 AND Reserves.H = 101
--SELECT * FROM Reserves, Boats WHERE  Boats.D < Reserves.H
SELECT * FROM Reserves, Sailors, Boats WHERE Reserves.H = Boats.D AND Sailors.A = Reserves.G