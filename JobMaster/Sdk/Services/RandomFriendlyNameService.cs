using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Services;

internal class RandomFriendlyNameService : JobMasterClusterAwareComponent, IRandomFriendlyNameService
{
    private readonly HashSet<string> animalsSelected = new();
    private readonly object objlock = new();

    public RandomFriendlyNameService(
        JobMasterClusterConnectionConfig clusterConnConfig) : base(clusterConnConfig)
    {
    }

    public string GetRandomFriendlyName(bool includeAdjective = true)
    {
        lock (objlock)
        {
            if (animalsSelected.Count >= FriendlyNameSelectedRecordConstants.Animals.Length)
            {
                animalsSelected.Clear();
            }

            var available = FriendlyNameSelectedRecordConstants.Animals
                .Where(a => !animalsSelected.Contains(a))
                .ToArray();

            var animal = available.Random()!;
            animalsSelected.Add(animal);

            if (!includeAdjective)
                return animal;

            var adjective = FriendlyNameSelectedRecordConstants.Adjectives.Random()!;

            return $"{adjective}-{animal}";
        }
    }
}

internal static class FriendlyNameSelectedRecordConstants
{
    public static readonly string[] Animals =
    {
        "aardwolf",
        "addax",
        "albatross",
        "arcticfox",
        "badger",
        "bear",
        "bison",
        "bobcat",
        "buffalo",
        "caracal",
        "caribou",
        "cheetah",
        "cobra",
        "condor",
        "cougar",
        "coyote",
        "dhole",
        "dingo",
        "eagle",
        "eland",
        "falcon",
        "ferret",
        "fox",
        "gazelle",
        "goshawk",
        "griffon",
        "hawk",
        "heron",
        "hyena",
        "ibex",
        "impala",
        "jackal",
        "jaguar",
        "kestrel",
        "kite",
        "kudu",
        "leopard",
        "lion",
        "lynx",
        "mamba",
        "marten",
        "meerkat",
        "moose",
        "mustang",
        "narwhal",
        "ocelot",
        "orca",
        "oryx",
        "otter",
        "owl",
        "panther",
        "peregrine",
        "polarfox",
        "polarbear",
        "polarwolf",
        "polarcat",
        "puma",
        "python",
        "raccoon",
        "raven",
        "rhino",
        "sable",
        "sandfox",
        "serval",
        "shark",
        "stallion",
        "stoat",
        "tiger",
        "urial",
        "viper",
        "walrus",
        "warthog",
        "weasel",
        "wolverine",
        "wolf",
        "yak",
        "zebra",
        "muskox",
        "harrier",
        "osprey",
        "buzzard"
    };

    public static readonly string[] Adjectives =
    {
        "agile",
        "bold",
        "brave",
        "calm",
        "daring",
        "fierce",
        "focused",
        "mighty",
        "noble",
        "rapid",
        "robust",
        "sharp",
        "silent",
        "solid",
        "steady",
        "swift",
        "wise",
        "sturdy",
        "keen",
        "lucid",
        "precise",
        "valiant",
        "resolute"
    };
}