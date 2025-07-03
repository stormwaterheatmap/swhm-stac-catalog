<template>
  <div class="hero-container">
    <div class="hero-content">
      <h1>Welcome to Our STAC Catalog</h1>
      <p>Explore our geospatial data collections</p>
    </div>
  </div>
</template>

<script>
export default {
  name: 'Hero'
};
</script>

<style lang="scss" scoped>
.hero-container {
  background-image: url('https://placehold.co/1200x400/000000/FFFFFF?text=Your+Hero+Image');
  background-size: cover;
  background-position: center;
  color: white;
  text-align: center;
  padding: 5rem 1rem;
  margin-bottom: 1.5rem;
}

.hero-content h1 {
  font-size: 2.5rem;
  font-weight: bold;
  margin-bottom: 1rem;
}

.hero-content p {
  font-size: 1.25rem;
}
</style>
