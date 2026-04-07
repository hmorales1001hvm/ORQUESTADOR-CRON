using System.Collections.Generic;
using System.Linq;

namespace Soltec.Orquestacion.BR
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<List<T>> ChunkBy<T>(
            this IEnumerable<T> source,
            int size)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (size <= 0)
                throw new ArgumentException("El tamaño debe ser mayor a 0");

            var chunk = new List<T>(size);

            foreach (var item in source)
            {
                chunk.Add(item);

                if (chunk.Count == size)
                {
                    yield return chunk;
                    chunk = new List<T>(size);
                }
            }

            if (chunk.Any())
                yield return chunk;
        }
    }
}
