namespace SqlStreamStore
{
    using System;

    internal class Link : IEquatable<Link>
    {
        public string Rel { get; set; }
        public string Href { get; set; }
        public string Title { get; set; }

        public bool Equals(Link other)
        {
            if(ReferenceEquals(null, other))
                return false;
            if(ReferenceEquals(this, other))
                return true;
            return string.Equals(Rel, other.Rel) 
                   && string.Equals(Href ?? string.Empty, other.Href ?? string.Empty)
                   && string.Equals(Title, other.Title);
        }

        public override bool Equals(object obj)
        {
            if(ReferenceEquals(null, obj))
                return false;
            if(ReferenceEquals(this, obj))
                return true;
            if(obj.GetType() != GetType())
                return false;
            return Equals((Link) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Rel?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Href?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Title?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public override string ToString() => new { Rel, Href, Title }.ToString();
    }
}