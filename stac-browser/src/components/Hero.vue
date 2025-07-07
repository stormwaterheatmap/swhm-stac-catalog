<template>
  <div class="hero-container" :style="heroStyle">
    <div class="hero-overlay"></div>
    <div class="hero-content">
      <h1>{{ title }}</h1>
      <p>{{ subtitle }}</p>
    </div>
  </div>
</template>

<script>
export default {
  name: 'Hero',
  props: {
    title: {
      type: String,
      default: 'Stormwater Heatmap STAC Catalog'
    },
    subtitle: {
      type: String,
      default: 'Explore geospatial data collections for stormwater management'
    },
    backgroundImage: {
      type: String,
      default: 'https://images.unsplash.com/photo-1446329813274-7c9036bd9a1f?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=2340&q=80'
    }
  },
  computed: {
    heroStyle() {
      return {
        backgroundImage: `url('${this.backgroundImage}')`
      };
    }
  }
};
</script>

<style lang="scss" scoped>
.hero-container {
  position: relative;
  background-size: cover;
  background-position: center;
  color: white;
  text-align: center;
  padding: 8rem 1rem;
  margin-bottom: 2rem;
  min-height: 400px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.hero-overlay {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.4);
  z-index: 1;
}

.hero-content {
  position: relative;
  z-index: 2;
  max-width: 800px;
}

.hero-content h1 {
  font-size: 3rem;
  font-weight: bold;
  margin-bottom: 1rem;
  text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.5);
}

.hero-content p {
  font-size: 1.5rem;
  text-shadow: 1px 1px 2px rgba(0, 0, 0, 0.5);
  margin-bottom: 0;
}

@media (max-width: 768px) {
  .hero-container {
    padding: 4rem 1rem;
    min-height: 300px;
  }
  
  .hero-content h1 {
    font-size: 2rem;
  }
  
  .hero-content p {
    font-size: 1.2rem;
  }
}
</style>
