package TermProj;

import java.util.*;
import java.util.concurrent.ForkJoinPool;

public class Benchmarker {

        public static void main(String[] args) {

                ForkJoinPool pool = ForkJoinPool.commonPool();

                int V = 8_500;
                int E = 70_000_000;
                int diff = (E - V) / 10;

                for (; E >= V; E -= diff) {

                        Random rng = new Random(9437);

                        
                        List<ParallelKKT.Edge> edges = new ArrayList<>(E);
                        for (int i = 0; i < E; i++) {
                                int u = rng.nextInt(V);
                                int v = rng.nextInt(V);
                                if (u == v)
                                        v = (v + 1) % V;
                                edges.add(new ParallelKKT.Edge(u, v, rng.nextDouble() * 20));
                        }

                        ParallelKKT.Graph graph = new ParallelKKT.Graph(V, edges);

                        
                        ParallelFilterKruskal.mst(graph, pool);
                        ParallelKKT.mst(graph, pool);
                        kruskal.mst(graph);

                        // ── Filter-Kruskal
                        long t1 = System.currentTimeMillis();
                        List<ParallelKKT.Edge> fkMst = ParallelFilterKruskal.mst(graph, pool);
                        long t2 = System.currentTimeMillis();

                        // ── KKT 
                        List<ParallelKKT.Edge> kktMst = ParallelKKT.mst(graph, pool);
                        long t3 = System.currentTimeMillis();

                        // ── Sequential Kruskal 
                        List<ParallelKKT.Edge> seqMst = kruskal.mst(graph);
                        long t4 = System.currentTimeMillis();

                        double fkWeight = ParallelFilterKruskal.totalWeight(fkMst);
                        double kktWeight = ParallelKKT.totalWeight(kktMst);
                        double seqWeight = kruskal.totalWeight(seqMst);

                        System.out.printf("=== Graph (%,d vertices, %,d edges) ===%n", V, E);

                        System.out.printf("Filter-Kruskal: %5d ms | weight: %.2f | edges: %d%n",
                                        t2 - t1, fkWeight, fkMst.size());

                        System.out.printf("KKT:             %5d ms | weight: %.2f | edges: %d%n",
                                        t3 - t2, kktWeight, kktMst.size());

                        System.out.printf("Sequential:      %5d ms | weight: %.2f | edges: %d%n",
                                        t4 - t3, seqWeight, seqMst.size());

                        System.out.printf("FK vs KKT match: %s%n",
                                        Math.abs(fkWeight - kktWeight) < 1e-6 ? "YES" : "NO (KKT randomized)");

                        System.out.printf("FK vs Seq match: %s%n",
                                        Math.abs(fkWeight - seqWeight) < 1e-6 ? "YES" : "NO (BUG!)");

                        System.out.printf("Parallelism: %d threads%n%n",
                                        pool.getParallelism());
                }
        }
}